// Collection jobs: per-source fetch runs and body extraction before analysis.
import type { PgBoss } from "pg-boss";
import { dajialaConfigured } from "../providers/dajiala.ts";
import { collectSource, collectXShard } from "../sources/collect.ts";
import { checkMpAccount } from "../sources/mp.ts";
import { handler, QUEUES, workHandlers, type QueueHandler } from "./queue.ts";
import { extractionHandler } from "./content.ts";

/** The collection queues and body extraction; only while COLLECT_ENABLED (the worker and the batch). */
export function sourceHandlers(): QueueHandler[] {
  return [
    handler(QUEUES.fetchSource, 8, 2, ({ sourceId, force }) => collectSource(sourceId, { force })),
    // One search per shard of X accounts; every SocialData caller shares its per-minute budget.
    handler(QUEUES.fetchXShard, 2, 2, ({ key, sourceIds }) => collectXShard(key, sourceIds)),
    // WeChat accounts through Dajiala, only when it is configured (as their scheduled checks); it allows a
    // few requests per second, and two accounts at a time stays well under it.
    ...(dajialaConfigured() ? [handler(QUEUES.mpCheck, 2, 2, ({ sourceId, reason }) => checkMpAccount(sourceId, reason ?? "schedule"))] : []),
    extractionHandler(),
  ];
}

export async function registerSourceJobs(boss: PgBoss) {
  await workHandlers(boss, sourceHandlers());
}
