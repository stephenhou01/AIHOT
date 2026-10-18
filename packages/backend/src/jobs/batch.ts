// The pipeline in bounded batches, for a host without an always-on worker (Vercel). Each call runs the
// worker's own handlers (jobs/*.ts) and schedules (jobs/schedules.ts) inside a time budget and says
// whether work is left; the caller repeats it until `done`. Jobs are claimed from the same pg-boss
// queues the worker polls, so overlapping calls split the work instead of repeating it, and a call
// cut off mid-job leaves the job to pg-boss's expiry and retry like a stopped worker.
//
// One pass: due maintenance schedules (collection scheduling, retry sweep, recovery) → the queues
// (collection → body → analysis → grouping → digests, pushes, media) until empty or out of time →
// once nothing is runnable, the schedules that read the finished intake (heat, links, translations,
// daily/weekly/monthly editions).
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { CronExpressionParser } from "cron-parser";
import { config } from "../config.ts";
import { sql } from "../db.ts";
import { logError } from "../lib/log-error.ts";
import { serverModules, type Scheduled } from "../modules.ts";
import { translatePending } from "../editorial/translate.ts";
import { triageSettings } from "../editorial/triage.ts";
import { composeDueReports } from "../reports/compose.ts";
import { analysisHandler } from "./content.ts";
import { eventHandlers } from "./events.ts";
import { notifyHandlers } from "./notify.ts";
import { publicationHandlers } from "./publication.ts";
import { sourceHandlers } from "./sources.ts";
import { currentSchedules } from "./schedules.ts";
import { ensureQueue, getBoss, QUEUES, recordRun, type QueueHandler } from "./queue.ts";

/** The longest a call may run before it stops starting work; the platform limit (vercel.json maxDuration 300 s) is above it. */
export const MAX_BATCH_MS = 285_000;
const DEFAULT_BATCH_MS = 270_000;
/** Work that becomes due this soon (a retry, a debounced digest) keeps the intake from being `done`. */
const HORIZON_SECONDS = 600;
/** Time a job needs left in the budget before it is claimed: model work runs several calls of up to 120 s each. */
const MODEL_RESERVE_MS = 120_000;
const JOB_RESERVE_MS = 60_000;
const MODEL_QUEUES = new Set<string>([QUEUES.analyze, QUEUES.group, QUEUES.digest]);

/** Schedules that read the finished intake: they run once no job is runnable. */
const AFTER_INTAKE = new Set(["hot.rank", "hot.snapshot", "stories.links", "content.translate", "reports.compose"]);
/** Cheap or self-limiting schedules that run on every pass instead of once per cron slot. */
const EVERY_PASS = new Set(["content.sweep", "hot.rank", "reports.compose"]);
const MODEL_SCHEDULES = new Set(["content.translate", "reports.compose"]);
/** Editions written per pass; more due ones keep the batch from being `done`. */
const REPORTS_PER_PASS = 2;
/** Time a schedule needs left in the budget; the default covers the database-only ones. */
const SCHEDULE_RESERVE_MS: Record<string, number> = { "reports.compose": 240_000, "content.translate": 200_000 };
const DEFAULT_SCHEDULE_RESERVE_MS = 45_000;

type Lane = { queue: string; concurrency: number; batchSize: number; model: boolean; run: (data: unknown[]) => Promise<unknown> };

export interface BatchReport {
  done: boolean;
  /** When not done and nothing is runnable now: seconds until the next deferred work is due. */
  retryAfterSeconds: number | null;
  startedAt: string;
  elapsedMs: number;
  budgetMs: number;
  valves: { collect: boolean; modelCalls: boolean };
  schedules: Array<{ name: string; status: "ok" | "failed" | "skipped"; ms?: number; reason?: string; result?: unknown }>;
  jobs: Record<string, { completed: number; failed: number }>;
  pending: PipelinePending;
  errors: string[];
}

export interface PipelinePending {
  /** Per queue: runnable now, claimed by a running call or worker, and deferred (a later start or retry). */
  queues: Record<string, { queued: number; active: number; deferred: number }>;
  sourcesDue: number;
  articlesAwaitingAnalysis: number;
  articlesAwaitingGrouping: number;
  /** Queues and schedules this deployment leaves alone because a valve is off. */
  blocked: string[];
}

const collecting = () => process.env.COLLECT_ENABLED === "true";

function lanes(): Lane[] {
  const engine: QueueHandler[] = [...(collecting() ? sourceHandlers() : []), analysisHandler(), ...eventHandlers(), ...notifyHandlers(), ...publicationHandlers()];
  return [
    ...engine.map((h) => ({ queue: h.queue, concurrency: h.concurrency, batchSize: 1, model: MODEL_QUEUES.has(h.queue),
      run: ([data]: unknown[]) => (h.run as (d: unknown) => Promise<unknown>)(data) })),
    ...serverModules().flatMap((m) => m.queues ?? []).map((q) => ({ queue: q.name, concurrency: q.worker.localConcurrency ?? 1,
      batchSize: q.worker.batchSize ?? 1, model: false, run: (data: unknown[]) => q.run(data as never[]) })),
  ];
}

/** A lease outlives its holder by this much when the platform cuts the call off; a live holder renews it. */
const LEASE_SECONDS = 90;

/** Lease rows shared with content pushes (delivery_leases): one statement each, so they hold behind any connection pooler. */
async function takeLease(key: string, holder: string): Promise<boolean> {
  const rows = await sql`
    INSERT INTO delivery_leases (lease_key, holder, expires_at) VALUES (${key}, ${holder}, now() + make_interval(secs => ${LEASE_SECONDS}))
    ON CONFLICT (lease_key) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
    WHERE delivery_leases.expires_at < now() OR delivery_leases.holder = EXCLUDED.holder
    RETURNING lease_key`;
  return rows.length > 0;
}

/** Runs `fn` while holding the lease, renewing it until `fn` settles; null when another call holds it. */
async function underLease<T>(key: string, holder: string, fn: () => Promise<T>): Promise<{ value: T } | null> {
  if (!(await takeLease(key, holder))) return null;
  const renew = setInterval(() => void takeLease(key, holder).catch((error) => console.error("[batch] lease", logError(error))), (LEASE_SECONDS / 3) * 1000);
  try {
    return { value: await fn() };
  } finally {
    clearInterval(renew);
    await sql`DELETE FROM delivery_leases WHERE lease_key = ${key} AND holder = ${holder}`;
  }
}

async function leaseLeftSeconds(key: string): Promise<number> {
  const [row] = await sql<{ s: number }[]>`
    SELECT GREATEST(1, ceil(extract(epoch FROM expires_at - now())))::int AS s FROM delivery_leases WHERE lease_key = ${key}`;
  return row?.s ?? 1;
}

/** The latest slot of a cron expression (Asia/Shanghai, as the worker schedules it) at or before `now`. */
function lastSlot(cron: string, now: Date): Date {
  return CronExpressionParser.parse(cron, { tz: "Asia/Shanghai", currentDate: new Date(now.getTime() + 1000) }).prev().toDate();
}

async function ranSince(name: string, since: Date): Promise<boolean> {
  return (await sql`SELECT 1 FROM job_runs WHERE job = ${name} AND started_at >= ${since} LIMIT 1`).length > 0;
}

/** A job's error as the job keeps it: pg-boss stores the error's own fields (receiptId) for recovery. */
const brief = (error: unknown) => String(error instanceof Error ? error.message : error).slice(0, 300);

/**
 * One bounded batch. `budgetMs` is how long it may keep starting work (capped at MAX_BATCH_MS); work
 * already started finishes, so the platform limit must leave room above it.
 */
export function runBatch(opts: { budgetMs?: number } = {}): Promise<BatchReport> {
  // Each call leaves a job_runs row ("pipeline.batch") with its report, like a scheduled task.
  return recordRun("pipeline.batch", () => batch(opts));
}

async function batch(opts: { budgetMs?: number }): Promise<BatchReport> {
  const started = Date.now();
  const budgetMs = Math.min(Math.max(opts.budgetMs ?? DEFAULT_BATCH_MS, 1000), MAX_BATCH_MS);
  const deadline = started + budgetMs;
  const left = () => deadline - Date.now();
  const holder = randomUUID();
  const modelCalls = config.modelCallsEnabled;
  const report: BatchReport = {
    done: false, retryAfterSeconds: null, startedAt: new Date(started).toISOString(), elapsedMs: 0, budgetMs,
    valves: { collect: collecting(), modelCalls }, schedules: [], jobs: {}, pending: null as never, errors: [],
  };
  const boss = await getBoss();
  // Jobs a cut-off call left active expire and retry here, as the worker's supervisor does it.
  await boss.supervise().catch((error) => report.errors.push(`supervise: ${brief(error)}`));

  const schedules = currentSchedules().filter((s) => modelCalls || !MODEL_SCHEDULES.has(s.name));
  let unfinished = false;
  let moreReports = false;
  const runSchedule = async (s: Scheduled) => {
    const reserve = SCHEDULE_RESERVE_MS[s.name] ?? DEFAULT_SCHEDULE_RESERVE_MS;
    const now = new Date();
    if (!EVERY_PASS.has(s.name) && (await ranSince(s.name, lastSlot(s.cron, now)))) return;
    if (left() < reserve) {
      unfinished = true;
      report.schedules.push({ name: s.name, status: "skipped", reason: "budget" });
      return;
    }
    // The batch's own limits for the two that call models: translations within the time left, a few editions per pass.
    const run = s.name === "content.translate" ? () => translatePending({ budgetMs: Math.max(0, left() - 200_000) })
      : s.name === "reports.compose" ? () => composeDueReports(new Date(), REPORTS_PER_PASS)
      : s.run;
    const t = Date.now();
    try {
      const held = await underLease(`batch:schedule:${s.name}`, holder, () => recordRun(s.name, run));
      if (!held) {
        report.schedules.push({ name: s.name, status: "skipped", reason: "running elsewhere" });
        if (AFTER_INTAKE.has(s.name)) unfinished = true;
        return;
      }
      if (s.name === "reports.compose" && (held.value as { generated: string[] }).generated.length >= REPORTS_PER_PASS) moreReports = true;
      report.schedules.push({ name: s.name, status: "ok", ms: Date.now() - t, result: held.value });
    } catch (error) {
      report.schedules.push({ name: s.name, status: "failed", ms: Date.now() - t });
      report.errors.push(`${s.name}: ${brief(error)}`);
    }
  };

  for (const s of schedules.filter((s) => !AFTER_INTAKE.has(s.name))) await runSchedule(s);

  const active = lanes().filter((l) => modelCalls || !l.model);
  for (const l of active) await ensureQueue(l.queue);
  // Grouping stays serial across calls: only the call holding its lease works that queue.
  let drained: boolean;
  const held = active.some((l) => l.queue === QUEUES.group) ? await underLease(`batch:queue:${QUEUES.group}`, holder, () => drain(active, { left, report })) : { value: await drain(active, { left, report }) };
  let groupingElsewhere = false;
  if (held) drained = held.value;
  else {
    // Another call is grouping: work the other queues; grouping is still to do.
    await drain(active.filter((l) => l.queue !== QUEUES.group), { left, report });
    drained = false;
    groupingElsewhere = true;
  }
  if (drained && (await runnable(active.map((l) => l.queue))) === 0) {
    for (const s of schedules.filter((s) => AFTER_INTAKE.has(s.name))) await runSchedule(s);
  } else {
    unfinished = true;
  }

  report.pending = await pipelinePending();
  const queues = active.map((l) => l.queue);
  const soon = await deferredUntil(queues);
  const busy = queues.some((q) => (report.pending.queues[q]?.queued ?? 0) + (report.pending.queues[q]?.active ?? 0) > 0);
  report.done = !unfinished && !moreReports && !busy && soon === null;
  if (!report.done && !busy && !unfinished && soon !== null) report.retryAfterSeconds = Math.max(1, Math.ceil((soon.getTime() - Date.now()) / 1000));
  // Only jobs running elsewhere (another call, or one that died until its job expires): come back later.
  const queuedHere = queues.some((q) => (report.pending.queues[q]?.queued ?? 0) > 0);
  if (!report.done && busy && !queuedHere && !unfinished && !moreReports) {
    report.retryAfterSeconds = Math.min(30, report.retryAfterSeconds ?? 30);
  }
  // Grouping held by another call (or by a call that died, until its lease lapses): ask again later
  // rather than straight away, unless this call had other work cut short.
  if (!report.done && groupingElsewhere && (await runnable(queues.filter((q) => q !== QUEUES.group))) === 0) {
    report.retryAfterSeconds = Math.min(LEASE_SECONDS / 3, await leaseLeftSeconds(`batch:queue:${QUEUES.group}`));
  }
  report.elapsedMs = Date.now() - started;
  return report;
}

/**
 * Claims and runs jobs, each queue with its worker concurrency, until no job is runnable and none is
 * running here (a running one may queue the next step), or the budget runs out. Returns whether it ran
 * out of work rather than time.
 */
async function drain(active: Lane[], ctx: { left: () => number; report: BatchReport }): Promise<boolean> {
  const boss = await getBoss();
  let running = 0;
  let outOfTime = false;
  const lane = async (l: Lane) => {
    const tally = (ctx.report.jobs[l.queue] ??= { completed: 0, failed: 0 });
    // An empty queue is asked again more slowly while other jobs run (they may queue its next job).
    let idleMs = 250;
    for (;;) {
      if (ctx.left() < (l.model ? MODEL_RESERVE_MS : JOB_RESERVE_MS)) {
        outOfTime = true;
        return;
      }
      const jobs = await boss.fetch<unknown>(l.queue, { batchSize: l.batchSize });
      if (jobs.length === 0) {
        if (running === 0) return;
        await delay(idleMs);
        idleMs = Math.min(idleMs * 2, 2000);
        continue;
      }
      idleMs = 250;
      running += 1;
      const ids = jobs.map((j) => j.id);
      try {
        const output = await l.run(jobs.map((j) => j.data));
        await boss.complete(l.queue, ids, output && typeof output === "object" ? output : { result: output });
        tally.completed += ids.length;
      } catch (error) {
        tally.failed += ids.length;
        ctx.report.errors.push(`${l.queue}: ${brief(error)}`);
        await boss.fail(l.queue, ids, error as object).catch((e) => console.error("[batch] fail", logError(e)));
      } finally {
        running -= 1;
      }
    }
  };
  await Promise.all(active.flatMap((l) => Array.from({ length: l.concurrency }, () => lane(l))));
  return !outOfTime;
}

async function runnable(queues: string[]): Promise<number> {
  const [row] = await sql<{ n: number }[]>`
    SELECT count(*)::int AS n FROM pgboss.job WHERE name = ANY(${queues}) AND state IN ('created', 'retry') AND start_after <= now()`;
  return row!.n;
}

/** The earliest deferred work within the horizon: a job's later start, or an article's retry time. */
async function deferredUntil(queues: string[]): Promise<Date | null> {
  const [row] = await sql<{ at: Date | null }[]>`
    SELECT least(
      (SELECT min(start_after) FROM pgboss.job WHERE name = ANY(${queues}) AND state IN ('created', 'retry')
         AND start_after > now() AND start_after <= now() + make_interval(secs => ${HORIZON_SECONDS})),
      (SELECT min(processing_retry_at) FROM articles WHERE processing_state = 'new'
         AND processing_retry_at > now() AND processing_retry_at <= now() + make_interval(secs => ${HORIZON_SECONDS}))) AS at`;
  return row!.at;
}

/** Before the first batch or worker start the queue tables do not exist yet; the read-only status must not create them. */
async function queuesInstalled(): Promise<boolean> {
  const [row] = await sql<{ table: string | null }[]>`SELECT to_regclass('pgboss.job')::text AS table`;
  return row!.table !== null;
}

export async function pipelinePending(): Promise<PipelinePending> {
  const all = lanes();
  const queues = all.map((l) => l.queue);
  const engineQueues = Object.values(QUEUES) as string[];
  const rows = !(await queuesInstalled()) ? [] : await sql<{ name: string; queued: number; active: number; deferred: number }[]>`
    SELECT name,
           count(*) FILTER (WHERE state IN ('created', 'retry') AND start_after <= now())::int AS queued,
           count(*) FILTER (WHERE state = 'active')::int AS active,
           count(*) FILTER (WHERE state IN ('created', 'retry') AND start_after > now())::int AS deferred
    FROM pgboss.job WHERE name = ANY(${[...new Set([...queues, ...engineQueues])]}) AND state IN ('created', 'retry', 'active')
    GROUP BY name`;
  const [counts] = await sql<{ sources_due: number; analysis: number; grouping: number }[]>`
    SELECT
      (SELECT count(*)::int FROM sources WHERE enabled AND kind IN ('rss', 'web_list', 'json_list', 'x_search')
         AND (next_fetch_at IS NULL OR next_fetch_at <= now())) AS sources_due,
      (SELECT count(*)::int FROM articles WHERE processing_state = 'new') AS analysis,
      (SELECT count(*)::int FROM articles WHERE grouping_status = 'pending' AND processing_state = 'analyzed'
         AND EXISTS (SELECT 1 FROM analyses an WHERE an.article_id = articles.id AND an.input_revision = articles.revision AND an.relevance = 'pass')) AS grouping`;
  const blocked = [
    ...(collecting() ? [] : ["collection (COLLECT_ENABLED)"]),
    ...(config.modelCallsEnabled ? [] : ["analysis, grouping, digests, translations and editions (MODEL_CALLS_ENABLED)"]),
    ...(await exhaustedBudgets()).map((b) => `${b.service} budget (${b.window}): its calls wait until the window frees up`),
  ];
  return {
    queues: Object.fromEntries(rows.map((r) => [r.name, { queued: r.queued, active: r.active, deferred: r.deferred }])),
    sourcesDue: counts!.sources_due,
    articlesAwaitingAnalysis: counts!.analysis,
    articlesAwaitingGrouping: counts!.grouping,
    blocked,
  };
}

/** Services whose circuit breaker is open now, by the same windows the receipts check (providers/receipts.ts). */
async function exhaustedBudgets(): Promise<Array<{ service: string; window: "stopped" | "minute" | "hour" | "day" }>> {
  const rows = await sql<{ service: string; per_minute: number; per_hour: number; per_day: number; minute: number; hour: number; day: number }[]>`
    SELECT b.service, b.per_minute, b.per_hour, b.per_day,
           count(a.id) FILTER (WHERE a.started_at > now() - interval '1 minute')::int AS minute,
           count(a.id) FILTER (WHERE a.started_at > now() - interval '1 hour')::int AS hour,
           count(a.id)::int AS day
    FROM budgets b
    LEFT JOIN receipt_attempts a ON a.service = b.service AND a.origin = 'live' AND a.started_at > now() - interval '1 day'
    GROUP BY b.service, b.per_minute, b.per_hour, b.per_day ORDER BY b.service`;
  return rows.flatMap((r) => {
    const window = r.per_minute <= 0 || r.per_hour <= 0 || r.per_day <= 0 ? "stopped" as const
      : r.day >= r.per_day ? "day" as const : r.hour >= r.per_hour ? "hour" as const : r.minute >= r.per_minute ? "minute" as const : null;
    return window ? [{ service: r.service, window }] : [];
  });
}

/** The read-only view for whoever triggers the batches: progress, recent trouble and paid usage. */
export async function pipelineStatus() {
  const [intake] = await sql<{ last_fetch_at: Date | null; last_ok_at: Date | null; last_schedule_at: Date | null; new_24h: number; selected_24h: number; stories_24h: number; triaged_24h: number; triage_dropped_24h: number }[]>`
    SELECT
      (SELECT max(last_fetch_at) FROM sources WHERE enabled) AS last_fetch_at,
      (SELECT max(last_ok_at) FROM sources WHERE enabled) AS last_ok_at,
      (SELECT max(started_at) FROM job_runs WHERE job = 'sources.schedule' AND status = 'ok') AS last_schedule_at,
      (SELECT count(*)::int FROM articles WHERE discovered_at > now() - interval '1 day') AS new_24h,
      (SELECT count(*)::int FROM publications WHERE selected AND discovered_at > now() - interval '1 day') AS selected_24h,
      (SELECT count(DISTINCT story_id)::int FROM publications WHERE story_id IS NOT NULL AND discovered_at > now() - interval '1 day') AS stories_24h,
      (SELECT count(*)::int FROM analyses WHERE created_at > now() - interval '1 day' AND output ? 'triage') AS triaged_24h,
      (SELECT count(*)::int FROM analyses WHERE created_at > now() - interval '1 day' AND (output->'triage'->>'dropped')::boolean) AS triage_dropped_24h`;
  const reports = await sql<{ kind: string; key: string; generated_at: Date }[]>`
    SELECT DISTINCT ON (kind) kind, key, generated_at FROM reports ORDER BY kind, generated_at DESC`;
  const failedRuns = await sql<{ job: string; started_at: Date; error: string | null }[]>`
    SELECT job, started_at, left(error, 300) AS error FROM job_runs WHERE status = 'failed' AND started_at > now() - interval '1 day'
    ORDER BY started_at DESC LIMIT 10`;
  const failedArticles = await sql<{ error: string; count: number }[]>`
    SELECT left(coalesce(processing_error, '(no message)'), 160) AS error, count(*)::int AS count FROM articles
    WHERE processing_state = 'failed' AND discovered_at > now() - interval '1 day' GROUP BY 1 ORDER BY 2 DESC LIMIT 5`;
  const failedJobs = !(await queuesInstalled()) ? [] : await sql<{ name: string; count: number }[]>`
    SELECT name, count(*)::int AS count FROM pgboss.job WHERE state = 'failed' AND completed_on > now() - interval '1 day' GROUP BY name ORDER BY 2 DESC`;
  const budgets = await sql<{ service: string; per_minute: number; per_hour: number; per_day: number; used_hour: number; used_day: number;
    prompt_tokens: number; completion_tokens: number; cost: string | null; currency: string | null }[]>`
    SELECT s.service, b.per_minute, b.per_hour, b.per_day,
           count(a.id) FILTER (WHERE a.started_at > now() - interval '1 hour')::int AS used_hour,
           count(a.id)::int AS used_day,
           coalesce(sum((a.usage->>'prompt_tokens')::bigint), 0)::bigint AS prompt_tokens,
           coalesce(sum((a.usage->>'completion_tokens')::bigint), 0)::bigint AS completion_tokens,
           sum(a.cost)::text AS cost, max(a.currency) AS currency
    FROM (SELECT service FROM budgets UNION SELECT DISTINCT service FROM receipt_attempts WHERE started_at > now() - interval '1 day') s
    LEFT JOIN budgets b ON b.service = s.service
    LEFT JOIN receipt_attempts a ON a.service = s.service AND a.origin = 'live' AND a.started_at > now() - interval '1 day'
    GROUP BY s.service, b.per_minute, b.per_hour, b.per_day ORDER BY s.service`;
  const [lastBatch] = await sql<{ started_at: Date; finished_at: Date | null; status: string; done: boolean | null; errors: string[] | null }[]>`
    SELECT started_at, finished_at, status, (detail->>'done')::boolean AS done, detail->'errors' AS errors
    FROM job_runs WHERE job = 'pipeline.batch' ORDER BY started_at DESC LIMIT 1`;
  const open = new Map((await exhaustedBudgets()).map((b) => [b.service, b.window]));
  return {
    valves: { collect: collecting(), modelCalls: config.modelCallsEnabled },
    triage: triageStatus(),
    lastCollectionAt: intake!.last_fetch_at,
    lastSuccessfulCollectionAt: intake!.last_ok_at,
    lastCollectionScheduledAt: intake!.last_schedule_at,
    lastBatch: lastBatch ? { startedAt: lastBatch.started_at, finishedAt: lastBatch.finished_at, status: lastBatch.status, done: lastBatch.done, errors: lastBatch.errors ?? [] } : null,
    last24h: { newArticles: intake!.new_24h, selected: intake!.selected_24h, stories: intake!.stories_24h, triaged: intake!.triaged_24h, droppedByTriage: intake!.triage_dropped_24h },
    latestReports: Object.fromEntries(reports.map((r) => [r.kind, { key: r.key, generatedAt: r.generated_at }])),
    pending: await pipelinePending(),
    recentErrors: { failedRuns, failedArticles, failedJobs },
    budgets: budgets.map((b) => ({
      service: b.service, limits: b.per_day === null ? null : { perMinute: b.per_minute, perHour: b.per_hour, perDay: b.per_day },
      usedHour: b.used_hour, usedDay: b.used_day, exhausted: open.get(b.service) ?? null,
      tokensDay: { prompt: Number(b.prompt_tokens), completion: Number(b.completion_tokens) },
      costDay: b.cost === null ? null : { amount: Number(b.cost), currency: b.currency },
    })),
  };
}

/** The triage settings in force, or the configuration error that would fail every triage. */
function triageStatus(): { enabled: boolean; model: string; threshold: number } | { error: string } {
  try {
    const { enabled, model, threshold } = triageSettings();
    return { enabled, model, threshold };
  } catch (error) {
    return { error: (error as Error).message };
  }
}
