// The on-demand pipeline (POST /api/pipeline/batch): repeated calls take a day's intake from collection
// through analysis and grouping to the after-intake schedules, overlapping calls never process an
// article twice, an idle call does nothing paid, the valves hold, and only the cron secret opens it.
import { pointModels, stub, tag } from "./setup.ts";
import { analysisStep, SELECTING_SCORE } from "./analysis-steps.ts";
import assert from "node:assert/strict";
import http from "node:http";
import { after, before, test } from "node:test";
import { config } from "@aihot/backend/config";
import { closeDb, sql } from "@aihot/backend/db";
import { ensureQueue, getBoss, QUEUES, stopBoss } from "@aihot/backend/jobs/queue";
import { buildApp } from "../apps/api/src/app.ts";

const T = tag();
const SOURCE = `batch-${T}`;
const SECRET = `cron-secret-${T}-0123456789`;
process.env.CRON_SECRET = SECRET;
config.allowPrivateNetworkFetch = true;

let items = [1, 2, 3];
// The feed and its article pages (body extraction reads them), all local.
const feed = http.createServer((req, res) => {
  const page = /^\/article\/(\d+)$/.exec(req.url ?? "")?.[1];
  if (page) {
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(`<!doctype html><html><head><title>AI lab ${T} releases model ${page}</title></head><body><article><h1>AI lab ${T} releases model ${page}</h1>` +
      `${"<p>AI lab release model " + page + ". The new model includes benchmark and price details for developers and enterprises.</p>".repeat(15)}</article></body></html>`);
    return;
  }
  res.writeHead(200, { "content-type": "application/rss+xml" });
  res.end(`<?xml version="1.0"?><rss version="2.0"><channel><title>Batch ${T}</title>${items.map((n) =>
    `<item><title>AI lab ${T} releases model ${n}</title><link>${base}/article/${n}</link><pubDate>${new Date(Date.now() - n * 3600_000).toUTCString()}</pubDate>` +
    `<description>AI lab release ${T} model ${n}. ${"The new model includes benchmark and price details. ".repeat(12)}</description></item>`).join("")}</channel></rss>`);
});
await new Promise<void>((resolve) => feed.listen(0, "127.0.0.1", () => resolve()));
const base = `http://127.0.0.1:${(feed.address() as { port: number }).port}`;
const feedUrl = `${base}/feed.xml`;

/** Analysis calls per article and step; other model requests (grouping, digests) are counted apart. */
const calls = new Map<string, number>();
let others = 0;
let hashes = 0;
const provider = await stub((_hit, request) => {
  const body = JSON.parse(request.body) as { input?: string[] };
  // Embeddings (the DashScope key of the chat presets turns them on): one direction per text, so no two texts recall each other.
  if (body.input) return { data: body.input.map((_text, index) => ({ index, embedding: Array.from({ length: 1024 }, (_v, i) => (i === (hashes++ % 1024) ? 1 : 0)) })) };
  let step: string;
  try {
    step = analysisStep(request.body);
  } catch {
    // Grouping: every candidate is a different model's release.
    others += 1;
    const text = String(JSON.parse(request.body).messages.at(-1)?.content ?? "");
    if (text.includes("【报道 A】")) return { choices: [{ message: { content: JSON.stringify({ a: "发布", b: "发布", relation: "UNRELATED", difference: "不同模型", confidence: 1 }) } }] };
    const ids = [...text.matchAll(/【候选 (C\d+)】/g)].map((m) => m[1]);
    return { choices: [{ message: { content: JSON.stringify({ query: "模型发布", decisions: ids.map((id) => ({ id, relation: "UNRELATED", confidence: 1, note: "" })), selection: { addsValue: true, reason: "fixture" } }) } }] };
  }
  const n = /model (\d+)/.exec(request.body)?.[1] ?? "?";
  calls.set(`${n}:${step}`, (calls.get(`${n}:${step}`) ?? 0) + 1);
  const content = step === "prefilter" ? { label: "PASS", reason: "fixture" }
    : step === "score" ? { attentionScore: SELECTING_SCORE }
    : step === "structure" ? { category: "ai-models", tags: ["模型发布"], subjects: [], scope: "single",
      fact: { title: `实验室发布模型 ${n}`, subject: "实验室", action: "发布", object: `模型 ${n}`, occurredAt: null, evidence: `AI lab release model ${n}`, conditions: [] } }
    : { itemType: "model_release", authorRole: "principal", tags: ["模型发布"], editorialJudgment: "模型能力提升", titleZh: `模型 ${n} 发布 ${T}`, summaryZh: `实验室发布模型 ${n}，并提供评测和价格。` };
  return { choices: [{ message: { content: JSON.stringify(content) } }] };
});
pointModels(provider.url, ["qwen3.7-flash", "glm-5.3-flash", "deepseek-flash", "qwen3.8-flash", "mimo-v2.6-flash"]);

const app = await buildApp();
const call = async (path = "/api/pipeline/batch", secret: string | null = SECRET, method: "GET" | "POST" = "POST") => {
  const res = await app.inject({ method, url: path, headers: secret === null ? {} : { authorization: `Bearer ${secret}` } });
  return { status: res.statusCode, body: res.statusCode === 200 ? res.json() : res.body };
};

before(async () => {
  // Today's edition exists, so the after-intake run has no edition to write in this fixture.
  const { dueDaily } = await import("@aihot/backend/reports/compose");
  await sql`INSERT INTO reports (kind, key, window_start, window_end, content, generated_at)
            VALUES ('daily', ${dueDaily()}, now() - interval '1 day', now(), '{}', now()) ON CONFLICT DO NOTHING`;
  await sql`INSERT INTO sources (id, name, kind, config, tier, participation_mode, cursor)
            VALUES (${SOURCE}, 'Batch', 'rss', ${sql.json({ feedUrl })}, 'T1', 'editorial',
                    ${sql.json({ initializedAt: new Date(Date.now() - 86400_000).toISOString() })})`;
});
after(async () => { await app.close(); feed.close(); await provider.close(); await stopBoss(); await closeDb(); });

/** Calls the batch until it reports done (at most `max` calls), letting deferred work fall due at once. */
async function drain(max = 8) {
  const reports = [];
  for (let i = 0; i < max; i++) {
    const { status, body } = await call();
    assert.equal(status, 200, String(body));
    reports.push(body);
    if (body.done) return reports;
    // The digest waits a minute after grouping; a caller would sleep retryAfterSeconds instead.
    if (body.retryAfterSeconds) await sql`UPDATE pgboss.job SET start_after = now() WHERE state IN ('created', 'retry') AND start_after > now()`;
  }
  assert.fail(`not done after ${max} calls: ${JSON.stringify(reports.at(-1))}`);
}

test("the batch and status refuse callers without the cron secret", async () => {
  assert.equal((await call("/api/pipeline/batch", null)).status, 401);
  assert.equal((await call("/api/pipeline/batch", "wrong-secret-0123456789")).status, 401);
  assert.equal((await call("/api/pipeline/status", null, "GET")).status, 401);
  process.env.CRON_SECRET = "";
  try {
    assert.equal((await call("/api/pipeline/batch", "")).status, 401, "no secret configured opens nothing");
  } finally {
    process.env.CRON_SECRET = SECRET;
  }
});

test("model calls switched off leave analysis untouched and say so", async () => {
  config.modelCallsEnabled = false;
  try {
    const { body } = await call();
    assert.ok(body.pending.blocked.some((b: string) => b.includes("MODEL_CALLS_ENABLED")));
    assert.equal(body.jobs["content.analyze"], undefined, "the analysis queue is not worked");
    assert.equal(calls.size + others, 0, "no model request");
    assert.ok(!body.schedules.some((s: { name: string }) => s.name === "reports.compose" || s.name === "content.translate"));
    const [{ n }] = await sql<{ n: number }[]>`SELECT count(*)::int AS n FROM articles WHERE source_id = ${SOURCE}`;
    assert.equal(n, 3, "collection still ran");
  } finally {
    config.modelCallsEnabled = true;
  }
});

test("repeated calls carry the intake through analysis and grouping, and an idle call does nothing", async () => {
  const reports = await drain();
  const last = reports.at(-1)!;
  assert.equal(last.done, true);
  const articles = await sql<{ processing_state: string; grouping_status: string; selected: boolean; story_id: number | null }[]>`
    SELECT a.processing_state, a.grouping_status, p.selected, p.story_id FROM articles a LEFT JOIN publications p ON p.article_id = a.id
    WHERE a.source_id = ${SOURCE}`;
  assert.equal(articles.length, 3);
  for (const a of articles) {
    assert.equal(a.processing_state, "analyzed");
    assert.equal(a.selected, true);
    assert.ok(a.story_id, "grouped into a story");
  }
  for (const n of [1, 2, 3]) assert.equal(calls.get(`${n}:prefilter`), 1, `article ${n} was judged once`);
  const ran = reports.flatMap((r) => r.schedules.filter((s: { status: string }) => s.status === "ok").map((s: { name: string }) => s.name));
  for (const name of ["content.sweep", "hot.rank", "reports.compose"]) assert.ok(ran.includes(name), `${name} ran`);
  assert.ok((await sql`SELECT 1 FROM job_runs WHERE job = 'sources.schedule' AND status = 'ok'`).length > 0, "collection was scheduled");
  assert.equal(last.pending.queues["content.analyze"]?.queued ?? 0, 0);

  const before = calls.size + others;
  const idle = (await call()).body;
  assert.equal(idle.done, true, "idle calls are done at once");
  assert.equal(Object.values(idle.jobs as Record<string, { completed: number }>).reduce((sum, j) => sum + j.completed, 0), 0);
  assert.equal(calls.size + others, before, "no paid request when idle");

  const status = (await call("/api/pipeline/status", SECRET, "GET")).body;
  assert.equal(status.last24h.newArticles, 3);
  assert.equal(status.last24h.selected, 3);
  assert.ok(status.lastCollectionAt);
  assert.equal(status.lastBatch.done, true);
  assert.ok(status.budgets.some((b: { service: string; usedDay: number }) => b.usedDay > 0), "paid requests are counted against their budget");
});

test("overlapping calls split new work without processing an article twice", async () => {
  items = [4, 5, 6, 7];
  // Due again, and its schedule's slot (once a minute) not yet taken.
  await sql`UPDATE sources SET next_fetch_at = now() WHERE id = ${SOURCE}`;
  await sql`DELETE FROM job_runs WHERE job = 'sources.schedule'`;
  const [a, b] = await Promise.all([call(), call()]);
  assert.equal(a.status, 200);
  assert.equal(b.status, 200);
  await drain();
  for (const n of [4, 5, 6, 7]) assert.equal(calls.get(`${n}:prefilter`), 1, `article ${n} was judged once`);
  const [{ stories }] = await sql<{ stories: number }[]>`
    SELECT count(*)::int AS stories FROM articles a JOIN publications p ON p.article_id = a.id WHERE a.source_id = ${SOURCE} AND p.story_id IS NOT NULL`;
  assert.equal(stories, 7);
});

test("grouping waits while another call holds it, and the call is not done", async () => {
  items = [9];
  await sql`UPDATE sources SET next_fetch_at = now() WHERE id = ${SOURCE}`;
  await sql`DELETE FROM job_runs WHERE job = 'sources.schedule'`;
  await sql`INSERT INTO delivery_leases (lease_key, holder, expires_at) VALUES ('batch:queue:events.group', 'another-call', now() + interval '5 minutes')`;
  try {
    const held = (await call()).body;
    assert.equal(held.done, false);
    assert.ok(held.retryAfterSeconds >= 1 && held.retryAfterSeconds <= 30, "asks the caller to come back");
    assert.equal(held.jobs["events.group"], undefined, "grouping left to the holder");
    assert.equal(held.jobs["content.analyze"].completed, 1, "the rest of the pipeline still runs");
    assert.ok(held.pending.queues["events.group"].queued >= 1);
  } finally {
    await sql`DELETE FROM delivery_leases WHERE lease_key = 'batch:queue:events.group'`;
  }
  await drain();
  const [{ story }] = await sql<{ story: number | null }[]>`
    SELECT p.story_id AS story FROM articles a JOIN publications p ON p.article_id = a.id WHERE a.source_id = ${SOURCE} AND a.url LIKE '%/article/9'`;
  assert.ok(story, "grouped once the lease is free");
});

test("a call without time left starts no work and is not done", async () => {
  items = [8];
  await sql`UPDATE sources SET next_fetch_at = now() WHERE id = ${SOURCE}`;
  await sql`DELETE FROM job_runs WHERE job = 'sources.schedule'`;
  const short = (await call("/api/pipeline/batch?budget=1")).body;
  assert.equal(short.budgetMs, 1000);
  assert.equal(short.done, false);
  assert.ok(short.schedules.some((s: { name: string; reason?: string }) => s.name === "sources.schedule" && s.reason === "budget"));
  assert.equal(Object.values(short.jobs as Record<string, { completed: number }>).reduce((sum, j) => sum + j.completed, 0), 0);
  await drain();
  assert.equal(calls.get("8:prefilter"), 1);
});

test("an open budget breaker stops paid calls, and the batch says so instead of spinning", async () => {
  items = [11];
  await sql`UPDATE sources SET next_fetch_at = now() WHERE id = ${SOURCE}`;
  await sql`DELETE FROM job_runs WHERE job = 'sources.schedule'`;
  const saved = await sql<{ service: string; per_minute: number; per_hour: number; per_day: number }[]>`SELECT service, per_minute, per_hour, per_day FROM budgets`;
  await sql`UPDATE budgets SET per_day = 0`;
  try {
    const before = calls.size + others;
    const reports = await drain();
    const last = reports.at(-1);
    assert.equal(calls.size + others, before, "no paid request past the breaker");
    assert.equal(calls.get("11:prefilter"), undefined);
    assert.ok(last.pending.blocked.some((b: string) => b.includes("budget (stopped)")), JSON.stringify(last.pending.blocked));
    const [{ state }] = await sql<{ state: string }[]>`
      SELECT processing_state AS state FROM articles WHERE source_id = ${SOURCE} AND url LIKE '%/article/11'`;
    assert.equal(state, "new", "the article waits for the window rather than failing");
    const status = (await call("/api/pipeline/status", SECRET, "GET")).body;
    assert.ok(status.budgets.some((b: { exhausted: string | null }) => b.exhausted === "stopped"));
  } finally {
    for (const b of saved) await sql`UPDATE budgets SET per_minute = ${b.per_minute}, per_hour = ${b.per_hour}, per_day = ${b.per_day} WHERE service = ${b.service}`;
  }
  // Once the window frees up (here: at once), the waiting article goes through exactly once.
  // (The sweep leaves articles younger than three minutes to the collector's own queueing.)
  await sql`UPDATE articles SET processing_retry_at = now(), created_at = now() - interval '5 minutes' WHERE source_id = ${SOURCE} AND url LIKE '%/article/11'`;
  await drain();
  assert.equal(calls.get("11:prefilter"), 1);
});

test("a job running elsewhere keeps the batch from being done, and the caller is asked to come back", async () => {
  await drain();
  const boss = await getBoss();
  await ensureQueue(QUEUES.digest);
  // Another call (or one that died) holds this job: claimed, not finished.
  await boss.send(QUEUES.digest, { storyId: "held-elsewhere" });
  const [job] = await boss.fetch(QUEUES.digest);
  assert.ok(job, "claimed by the other caller");
  const before = calls.size + others;
  const busy = (await call()).body;
  assert.equal(busy.done, false);
  assert.ok(busy.retryAfterSeconds >= 1 && busy.retryAfterSeconds <= 30, `retryAfterSeconds ${busy.retryAfterSeconds}`);
  assert.equal(busy.pending.queues[QUEUES.digest].active, 1);
  assert.equal(calls.size + others, before, "nothing paid while waiting");
  await boss.complete(QUEUES.digest, job!.id);
  assert.equal((await call()).body.done, true, "done once the other caller finishes");
});
