// Bailian's decision model (decision-model-preview): not a chat model. POST {base}/systemone takes a
// `state` and typed questions (choice / noul / score) and answers each with probabilities in one
// forward pass, no text. Calls go through receipts like every paid request, on their own budget row
// (`decision`, added with DECISION_BUDGET on first use). Address and key default to the deployment's model (LLM_BASE_URL / LLM_API_KEY).
import { config, credential } from "../config.ts";
import { sql } from "../db.ts";
import { sha256 } from "../lib/ids.ts";
import { assertAccepted, paidRequest, ProviderRejectedError, rejectReceivedResponse } from "./receipts.ts";

export const DECISION_SERVICE = "decision";
/** The breaker's starting limits. Release migrations cannot seed rows, so the first call adds it; the admin's settings page changes it. */
export const DECISION_BUDGET = { perMinute: 60, perHour: 1000, perDay: 3000 };

let budgetSeeded = false;
async function seedBudget(): Promise<void> {
  if (budgetSeeded) return;
  await sql`
    INSERT INTO budgets (service, per_minute, per_hour, per_day, note)
    VALUES (${DECISION_SERVICE}, ${DECISION_BUDGET.perMinute}, ${DECISION_BUDGET.perHour}, ${DECISION_BUDGET.perDay}, '决策模型熔断（精选前的价值初筛 triage）')
    ON CONFLICT (service) DO NOTHING`;
  budgetSeeded = true;
}

export type DecisionQuestion =
  | { type: "choice"; instructions?: string; criteria: Record<string, string> }
  | { type: "noul"; instructions?: string; criteria?: { true?: string; false?: string } }
  | { type: "score"; instructions?: string; criteria: string[] };

export interface DecisionAnswer {
  type: "choice" | "noul" | "score";
  choice?: string;
  noul?: number;
  score?: number;
  probabilities?: Record<string, number>;
  confidence?: number;
}

export interface DecisionOptions {
  model: string;
  purpose: string;
  subject: string;
  promptVersion: string;
  state: string | Record<string, unknown>;
  questions: Record<string, DecisionQuestion>;
  attemptTag?: string;
  timeoutMs?: number;
}

export interface DecisionResult {
  answers: Record<string, DecisionAnswer>;
  receiptId: number;
  reused: boolean;
  usage: Record<string, unknown> | null;
}

export class DecisionOutputError extends Error {
  readonly receiptId: number | null;
  constructor(message: string, receiptId: number | null) {
    super(message);
    this.receiptId = receiptId;
  }
}

/** Where the decision model is reached: its own variables if set, else the default model's. */
function endpoint(): { baseUrl: string; apiKey: string } {
  const baseUrl = credential("models", "TRIAGE_BASE_URL") ?? credential("models", "LLM_BASE_URL");
  const apiKey = credential("models", "TRIAGE_API_KEY") ?? credential("models", "LLM_API_KEY");
  if (!baseUrl || !apiKey) throw new Error("Decision model is not configured (TRIAGE_BASE_URL / TRIAGE_API_KEY, or LLM_BASE_URL / LLM_API_KEY)");
  return { baseUrl: baseUrl.replace(/\/$/, ""), apiKey };
}

export async function decide(opts: DecisionOptions): Promise<DecisionResult> {
  if (!config.modelCallsEnabled) throw new Error("Model calls are disabled (MODEL_CALLS_ENABLED=false)");
  const { baseUrl, apiKey } = endpoint();
  await seedBudget();
  const stateText = typeof opts.state === "string" ? opts.state : JSON.stringify(opts.state);
  const questionsText = JSON.stringify(opts.questions);
  const receipt = await paidRequest(
    {
      service: DECISION_SERVICE,
      model: opts.model,
      purpose: opts.purpose,
      subject: opts.subject,
      identity: { model: opts.model, promptVersion: opts.promptVersion, state: sha256(stateText), questions: sha256(questionsText) },
      requestSummary: { promptVersion: opts.promptVersion, stateHash: sha256(stateText), stateChars: stateText.length, questions: Object.keys(opts.questions) },
      attemptTag: opts.attemptTag,
    },
    async () => {
      const started = Date.now();
      const res = await fetch(`${baseUrl}/systemone`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${apiKey}` },
        body: JSON.stringify({ model: opts.model, state: opts.state, questions: opts.questions }),
        signal: AbortSignal.timeout(opts.timeoutMs ?? 20_000),
      }).catch((error: unknown) => {
        const code = (error as { cause?: { code?: string } })?.cause?.code;
        if (code && ["ECONNREFUSED", "ENOTFOUND", "EAI_AGAIN", "UND_ERR_CONNECT_TIMEOUT"].includes(code)) {
          throw new ProviderRejectedError(`connect failed: ${String(error)}`, null, true);
        }
        throw error;
      });
      const text = await res.text();
      assertAccepted(DECISION_SERVICE, res.status, text);
      let json: Record<string, unknown>;
      try {
        json = JSON.parse(text);
      } catch {
        json = { unparsable: text.slice(0, 20000) };
      }
      return {
        response: { ...json, _latencyMs: Date.now() - started },
        requestId: (json.request_id as string | undefined) ?? res.headers.get("x-request-id"),
        usage: recordedUsage(json.usage),
        cost: null,
      };
    },
  );
  const response = receipt.response as { answers?: Record<string, DecisionAnswer>; usage?: Record<string, unknown> };
  const answers = response.answers;
  const missing = Object.keys(opts.questions).filter((q) => !answers?.[q]);
  if (!answers || missing.length) {
    await rejectReceivedResponse(receipt.receiptId, `unusable output: no answer for ${missing.join(", ") || "any question"}`);
    throw new DecisionOutputError(`Decision model ${opts.model} gave no answer for ${missing.join(", ")} (${opts.subject})`, receipt.receiptId);
  }
  return { answers, receiptId: receipt.receiptId, reused: receipt.reused, usage: response.usage ?? null };
}

/** The decision API reports `input_tokens`; token totals (status, admin models) read `prompt_tokens`. */
function recordedUsage(usage: unknown): Record<string, unknown> | null {
  if (!usage || typeof usage !== "object") return null;
  const u = usage as Record<string, unknown>;
  return typeof u.input_tokens === "number" && u.prompt_tokens === undefined
    ? { ...u, prompt_tokens: u.input_tokens, completion_tokens: 0 }
    : u;
}
