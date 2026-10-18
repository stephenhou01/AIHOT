// The worker's timers for the schedules (jobs/schedules.ts). Each run is recorded in job_runs; missed
// slots run once where the schedule asks for it.
import type { PgBoss } from "pg-boss";
import { ensureQueue, recordRun } from "@aihot/backend/jobs/queue";
import { currentSchedules } from "@aihot/backend/jobs/schedules";

export async function registerSchedules(boss: PgBoss) {
  const schedules = currentSchedules();
  for (const s of schedules) {
    const queue = `cron.${s.name}`;
    await ensureQueue(queue, { policy: "singleton", retryLimit: 1, expireInSeconds: 3600 });
    await boss.schedule(queue, s.cron, {}, { tz: "Asia/Shanghai", missed: s.missed ?? "skip" });
    // Schedules fire at minute boundaries; a 15 s pickup keeps them on time with a third of the polling.
    await boss.work(queue, { pollingIntervalSeconds: 15 }, async () => recordRun(s.name, s.run));
  }
  // Retired or disabled timers must leave no queued work behind, including queues an earlier release
  // already unscheduled. pg-boss deletes the queue's jobs and schedules; job_runs retains its audit.
  const current = new Set(schedules.map((s) => `cron.${s.name}`));
  for (const old of await boss.getQueues()) {
    if (old.name.startsWith("cron.") && !current.has(old.name)) await boss.deleteQueue(old.name);
  }
}
