// Triage (TRIAGE_ENABLED): after the prefilter passes, the decision model's value score stops low-value
// articles before the paid steps; a failed decision call (error, timeout, budget) lets the article through.
import { Reply, stub, tag } from "./setup.ts";
import { analysisStep, SELECTING_SCORE, type AnalysisStep } from "./analysis-steps.ts";
import assert from "node:assert/strict";
import { after, afterEach, before, test } from "node:test";
import { closeDb, sql } from "@aihot/backend/db";
import { upsertMaterial } from "@aihot/backend/content/materials";
import { analyzeArticle } from "@aihot/backend/editorial/analyze";
import { CAPABILITIES, type Capability } from "@aihot/backend/editorial/models";
import { triageSettings } from "@aihot/backend/editorial/triage";
import { stopBoss } from "@aihot/backend/jobs/queue";

for (const c of Object.values(CAPABILITIES) as Capability[]) {
  if (c.default === "default") delete process.env[c.env];
  else process.env[c.env] = "default";
}

const T = tag();
const SOURCE = `test-triage-${T}`;
let steps: AnalysisStep[] = [];
let decisions: Array<{ model: string; state: string; questions: Record<string, unknown> }> = [];
/** What the decision endpoint does next. */
let decision: { value: number } | { status: number } | { hangMs: number } = { value: 7 };

const provider = await stub(async (_hit, req) => {
  if (req.url.endsWith("/systemone")) {
    decisions.push(JSON.parse(req.body));
    if ("status" in decision) return new Reply(decision.status, { error: { message: "stub refusal" } });
    const now = decision;
    if ("hangMs" in now) await new Promise((r) => setTimeout(r, now.hangMs));
    const value = "value" in now ? now.value : 9;
    return { model: "decision-model-preview", answers: { value: { type: "score", score: value, confidence: 0.8, probabilities: {} } }, usage: { input_tokens: 42 } };
  }
  const step = analysisStep(req.body);
  steps.push(step);
  const content =
    step === "prefilter" ? { label: "PASS", reason: "测试" }
    : step === "score" ? { attentionScore: SELECTING_SCORE }
    : step === "understand" ? { itemType: "product_launch", authorRole: "principal", tags: ["产品更新"], editorialJudgment: "理由", titleZh: "一个模型的标题", summaryZh: "一个模型写的摘要。第二句。" }
    : step === "structure" ? { category: "ai-products", tags: ["产品更新"], subjects: [], fact: null }
    : "title_zh: 标题\nsummary_zh: 摘要。";
  return { id: `stub-${steps.length}`, choices: [{ message: { content: typeof content === "string" ? content : JSON.stringify(content) } }], usage: { prompt_tokens: 1, completion_tokens: 1 } };
});
Object.assign(process.env, {
  LLM_BASE_URL: `${provider.url}/v1`, LLM_API_KEY: "test-key", LLM_MODEL: "one-model", MODEL_CALLS_ENABLED: "true",
  TRIAGE_ENABLED: "true", TRIAGE_THRESHOLD: "5.5", TRIAGE_TIMEOUT_MS: "300",
});
delete process.env.TRIAGE_MODEL;
delete process.env.TRIAGE_BASE_URL;
delete process.env.TRIAGE_API_KEY;

before(async () => {
  await sql`INSERT INTO sources (id, name, kind, tier, participation_mode, next_fetch_at) VALUES (${SOURCE}, 'Test triage', 'rss', 'T1', 'editorial', '2100-01-01')`;
});
afterEach(() => {
  steps = [];
  decisions = [];
  decision = { value: 7 };
  process.env.TRIAGE_ENABLED = "true";
});
after(async () => {
  await provider.close();
  await stopBoss();
  await closeDb();
});

let n = 0;
async function article() {
  n += 1;
  const { articleId } = await upsertMaterial({
    sourceId: SOURCE, url: `https://example.com/${T}/${n}`, title: `A product launch ${T} ${n}`,
    bodyText: `A company launched a product with pricing and availability. ${T} ${n} `.repeat(6), bodyStatus: "ok", via: "fetch", publishedAt: new Date(),
  } as never);
  return articleId;
}

async function stored(articleId: string) {
  const [row] = await sql<{ state: string; relevance: string; reason_zh: string | null; triage: Record<string, unknown> | null }[]>`
    SELECT a.processing_state AS state, an.relevance, an.reason_zh, an.output->'triage' AS triage
    FROM articles a JOIN analyses an ON an.article_id = a.id WHERE a.id = ${articleId} ORDER BY an.created_at DESC LIMIT 1`;
  return row!;
}

test("settings: off unless enabled, decision-model-preview at 5.5 by default", () => {
  assert.deepEqual(triageSettings({}), { enabled: false, model: "decision-model-preview", threshold: 5.5, timeoutMs: 20_000 });
  assert.equal(triageSettings({ TRIAGE_ENABLED: "true", TRIAGE_THRESHOLD: "6" }).threshold, 6);
  assert.throws(() => triageSettings({ TRIAGE_THRESHOLD: "eleven" }));
});

test("below the threshold the article stops like a BLOCK, before the paid steps", async () => {
  decision = { value: 3.2 };
  const id = await article();
  const res = await analyzeArticle(id);
  assert.equal(res!.output!.relevance, "block");
  assert.equal(res!.output!.selected, false);
  assert.deepEqual(steps, ["prefilter"], "no score, structure or writing call");
  assert.equal(decisions.length, 1);
  assert.equal(decisions[0]!.model, "decision-model-preview");
  assert.ok(decisions[0]!.state.includes(`A product launch ${T}`), "the decision model reads the material");
  const row = await stored(id);
  assert.equal(row.state, "blocked");
  assert.equal(row.reason_zh, "low value (triage 3.20)");
  assert.equal(row.triage!.score, 3.2);
  assert.equal(row.triage!.dropped, true);
  const receipts = await sql<{ service: string; status: string }[]>`SELECT service, status FROM receipts WHERE subject LIKE ${`article:${id}%`} AND service = 'decision'`;
  assert.deepEqual(receipts.map((r) => ({ ...r })), [{ service: "decision", status: "completed" }]);
  const [budget] = await sql`SELECT per_minute, per_hour, per_day FROM budgets WHERE service = 'decision'`;
  assert.deepEqual({ ...budget }, { per_minute: 60, per_hour: 1000, per_day: 3000 }, "the breaker's row is added on first use");
});

test("at or above the threshold the analysis goes on, and the score is recorded", async () => {
  decision = { value: 5.5 };
  const id = await article();
  const res = await analyzeArticle(id);
  assert.equal(res!.output!.selected, true);
  assert.ok(steps.includes("score") && steps.includes("structure"));
  const row = await stored(id);
  assert.equal(row.state, "analyzed");
  assert.deepEqual({ score: row.triage!.score, dropped: row.triage!.dropped, threshold: row.triage!.threshold }, { score: 5.5, dropped: false, threshold: 5.5 });
});

for (const [name, setup] of [
  ["a provider error", () => { decision = { status: 500 }; }],
  ["a refusal", () => { decision = { status: 400 }; }],
  ["a timeout", () => { decision = { hangMs: 1500 }; }],
] as const) {
  test(`${name} from the decision model lets the article through without a score`, async () => {
    setup();
    const id = await article();
    const res = await analyzeArticle(id);
    assert.equal(res!.output!.selected, true, "judged as if triage were off");
    assert.ok(steps.includes("score"));
    const row = await stored(id);
    assert.equal(row.triage!.score, null);
    assert.equal(row.triage!.dropped, false);
    assert.ok(String(row.triage!.error).length > 0, "the failure is recorded");
  });
}

test("a spent decision budget lets the article through without a call", async () => {
  await sql`INSERT INTO budgets (service, per_minute, per_hour, per_day) VALUES ('decision', 60, 1000, 0)
            ON CONFLICT (service) DO UPDATE SET per_day = 0`;
  try {
    decision = { value: 1 };
    const id = await article();
    const res = await analyzeArticle(id);
    assert.equal(decisions.length, 0, "the breaker stops the call");
    assert.equal(res!.output!.selected, true);
    const row = await stored(id);
    assert.match(String(row.triage!.error), /Budget for decision exhausted/);
  } finally {
    await sql`UPDATE budgets SET per_day = 3000 WHERE service = 'decision'`;
  }
});

test("with TRIAGE_ENABLED off nothing is sent to the decision model", async () => {
  process.env.TRIAGE_ENABLED = "false";
  decision = { value: 0 };
  const id = await article();
  const res = await analyzeArticle(id);
  assert.equal(decisions.length, 0);
  assert.equal(res!.output!.selected, true);
  assert.equal((await stored(id)).triage, null);
});
