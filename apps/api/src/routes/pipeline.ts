// The pipeline on demand, for a deployment without an always-on worker (Vercel): an outside scheduler
// calls the batch until it answers `done: true`, and reads the status. Both take the cron secret
// (CRON_SECRET) as a bearer token; without one configured the routes refuse everything.
import { timingSafeEqual } from "node:crypto";
import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";
import { credential } from "@aihot/backend/config";
import { MAX_BATCH_MS, pipelineStatus, runBatch } from "@aihot/backend/jobs/batch";

const PLACEHOLDER = /^(|changeme|change-me|placeholder|xxx+|todo|test|dev|your[-_]?(token|secret).*)$/i;

/** Constant-time check; an empty, short or placeholder server secret rejects everything. */
export function cronAuthorized(req: FastifyRequest): boolean {
  const expected = credential("auth", "CRON_SECRET") ?? "";
  if (PLACEHOLDER.test(expected) || expected.length < 16) return false;
  const given = /^Bearer\s+(.+)$/i.exec(req.headers.authorization ?? "")?.[1]?.trim() ?? "";
  const a = Buffer.from(given);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

const refuse = (reply: FastifyReply) => reply.code(401).header("Cache-Control", "no-store").type("text/plain; charset=utf-8").send("Unauthorized");

export function registerPipeline(app: FastifyInstance) {
  // GET as well as POST, so a plain cron caller (Vercel Cron sends GET with this bearer token) can drive it.
  const batch = async (req: FastifyRequest, reply: FastifyReply) => {
    reply.header("Cache-Control", "no-store");
    if (!cronAuthorized(req)) return refuse(reply);
    // ?budget=<seconds> shortens a call (tests, a scheduler with a shorter timeout); never past the cap.
    const seconds = Number((req.query as { budget?: string }).budget);
    const budgetMs = Number.isFinite(seconds) && seconds > 0 ? Math.min(seconds * 1000, MAX_BATCH_MS) : undefined;
    try {
      return { ok: true, ...(await runBatch({ budgetMs })) };
    } catch (error) {
      req.log.error({ err: error }, "pipeline batch failed");
      return reply.code(500).send({ ok: false, done: false, error: "batch failed; see the function log and /api/pipeline/status" });
    }
  };
  app.post("/api/pipeline/batch", batch);
  app.get("/api/pipeline/batch", batch);
  app.get("/api/pipeline/status", async (req, reply) => {
    reply.header("Cache-Control", "no-store");
    if (!cronAuthorized(req)) return refuse(reply);
    return { ok: true, ...(await pipelineStatus()) };
  });
}
