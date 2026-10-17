// Cron-style schedules (Asia/Shanghai), the engine's and then the site's modules'. The worker runs them on
// pg-boss timers (apps/worker/src/schedules.ts); the on-demand batch runs the due ones itself (jobs/batch.ts).
import { responder, serverModules, type Scheduled } from "../modules.ts";
import { sweepUnprocessed } from "./content.ts";
import { translatePending } from "../editorial/translate.ts";
import { adaptIntervals, scheduleDueSources } from "../sources/collect.ts";
import { scheduleMpReconcile } from "../sources/mp.ts";
import { dajialaConfigured } from "../providers/dajiala.ts";
import { refreshSourceIcons } from "../sources/icons.ts";
import { computeHotRanking, snapshotHeat } from "../events/hot.ts";
import { linkRelatedStories } from "../events/consolidate.ts";
import { composeDueReports } from "../reports/compose.ts";
import { dailyRetention } from "../operations/retention.ts";
import { submitIndexNow } from "../operations/indexnow.ts";
import { checkAlerts, sendDigest } from "../operations/alerts.ts";
import { recoverStaleWork } from "../operations/recover.ts";
import { forwardPendingFeedback } from "../operations/feedback.ts";
import { backupConfigured, runBackup } from "../operations/backup.ts";
import { sourceHealthWeekly } from "../operations/reports.ts";

/** The engine's schedules, then the modules', without the ones switched off (`when`). */
export function currentSchedules(): Scheduled[] {
  return [...engineSchedules(), ...serverModules().flatMap((m) => m.schedules ?? [])].filter((s) => s.when?.() ?? true);
}

function engineSchedules(): Scheduled[] {
  const collecting = process.env.COLLECT_ENABLED === "true";
  return [
    { name: "content.sweep", cron: "*/5 * * * *", run: sweepUnprocessed },
    // Full-text translations of newly selected items (model calls; off with MODEL_CALLS_ENABLED=false).
    { name: "content.translate", cron: "*/5 * * * *", run: () => translatePending() },
    { name: "hot.rank", cron: "*/5 * * * *", run: () => computeHotRanking() },
    { name: "hot.snapshot", cron: "2 * * * *", run: () => snapshotHeat() },
    { name: "stories.links", cron: "12 * * * *", run: linkRelatedStories },
    // Every issue that is due and not written yet, each from its edition time (site/site.ts EDITION_TIMES)
    // at the next half hour; a missed or failed one at the next run.
    { name: "reports.compose", cron: "0,30 * * * *", missed: "once", run: () => composeDueReports() },
    // The deletions the privacy notice promises, once their retention periods are over.
    { name: "ops.retention", cron: "30 3 * * *", missed: "once", run: () => dailyRetention() },
    // IndexNow for added, changed and removed discovery URLs (off unless INDEXNOW_SUBMIT_ENABLED).
    { name: "seo.indexnow", cron: "50 5 * * *", missed: "once", run: () => submitIndexNow() },
    // Work a stopped process left half way becomes visible, and unknown paid requests get their one
    // automatic release; ops.alerts runs in parallel and sees the result by its next run at the latest.
    { name: "ops.recover", cron: "*/10 * * * *", run: () => recoverStaleWork() },
    { name: "ops.alerts", cron: "*/10 * * * *", run: () => checkAlerts() },
    // One message with other follow-ups and their actual impact (nothing when there are none); a site's
    // responder supplies its own notification policy instead.
    { name: "ops.digest", cron: "0 9 * * *", missed: "once", run: () => sendDigest(), when: () => !responder() },
    // Feedback that did not reach the internal Feishu chat when it was sent (off with FEISHU_INTERNAL_ENABLED).
    { name: "feedback.forward", cron: "*/10 * * * *", run: () => forwardPendingFeedback() },
    ...(backupConfigured() ? [{ name: "ops.backup", cron: "10 4 * * *", missed: "once" as const, run: () => runBackup() }] : []),
    { name: "reports.source-health", cron: "0 9 * * 1", missed: "once", run: () => sourceHealthWeekly(), when: () => !responder() },
    ...(collecting
      ? [
          { name: "sources.schedule", cron: "* * * * *", run: () => scheduleDueSources() },
          { name: "sources.adapt-intervals", cron: "20 4 * * *", run: adaptIntervals },
          // Icons are read from the sources' own sites, so they stop with collection.
          { name: "sources.icons", cron: "40 4 * * *", missed: "once" as const, run: () => refreshSourceIcons() },
        ]
      : []),
    // WeChat official accounts through Dajiala (paid), each once per its interval; only with its key.
    ...(collecting && dajialaConfigured() ? [{ name: "sources.mp-reconcile", cron: "*/15 * * * *", run: () => scheduleMpReconcile() }] : []),
  ];
}
