// Issues the scheduled run passed over because they had nothing to carry. Each is a settings row
// (reports.skipped.<kind>:<key>), so nothing is published for it and later runs do not try it again.
// Deleting the row lets a later scheduled run try that issue again, when it still lies in the range
// the run fills (from the kind's first issue or skip to the one due).
import { sql } from "../db.ts";

export type ReportKind = "daily" | "weekly" | "monthly";

/**
 * An issue with nothing to carry: a daily whose window has nothing judged in it, a weekly or monthly
 * without daily entries. An explicit regeneration reports it as an error; the scheduled run records the
 * issue as skipped (see composeDueReports) and publishes nothing for it.
 */
export class NothingToReportError extends Error {
  readonly window: { start: Date; end: Date };
  constructor(message: string, window: { start: Date; end: Date }) {
    super(message);
    this.window = window;
    this.name = "NothingToReportError";
  }
}

/** Settings key of a skipped issue: one row each, so concurrent runs cannot lose one. */
export const SKIPPED_PREFIX = "reports.skipped.";
export const skippedKey = (kind: ReportKind, key: string) => `${SKIPPED_PREFIX}${kind}:${key}`;

export interface SkippedReport { kind: ReportKind; key: string; reason: string; windowStart: string; windowEnd: string; at: string }

/** Records an issue the scheduled run passed over; the first record stays. */
export async function recordSkipped(kind: ReportKind, key: string, error: NothingToReportError): Promise<void> {
  const value: SkippedReport = { kind, key, reason: error.message, windowStart: error.window.start.toISOString(), windowEnd: error.window.end.toISOString(), at: new Date().toISOString() };
  await sql`INSERT INTO settings (key, value, updated_by) VALUES (${skippedKey(kind, key)}, ${sql.json(value as never)}, 'reports') ON CONFLICT (key) DO NOTHING`;
}

/** The keys of the issues of a kind the scheduled run skipped. */
export async function skippedKeys(kind: ReportKind): Promise<Set<string>> {
  const prefix = `${SKIPPED_PREFIX}${kind}:`;
  const rows = await sql<{ key: string }[]>`SELECT key FROM settings WHERE starts_with(key, ${prefix})`;
  return new Set(rows.map((r) => r.key.slice(prefix.length)));
}

/** The skipped issues, newest record first (status and alerts). */
export async function skippedReports(limit = 20): Promise<SkippedReport[]> {
  const rows = await sql<{ value: SkippedReport }[]>`
    SELECT value FROM settings WHERE starts_with(key, ${SKIPPED_PREFIX}) ORDER BY updated_at DESC, key DESC LIMIT ${limit}`;
  return rows.map((r) => r.value);
}

/** Whether the scheduled run skipped the daily of a date (an alert about it is then closed, not repeated). */
export async function dailySkipped(dates: string[]): Promise<Set<string>> {
  if (!dates.length) return new Set();
  const rows = await sql<{ key: string }[]>`SELECT key FROM settings WHERE key IN ${sql(dates.map((d) => skippedKey("daily", d)))}`;
  return new Set(rows.map((r) => r.key.slice(skippedKey("daily", "").length)));
}
