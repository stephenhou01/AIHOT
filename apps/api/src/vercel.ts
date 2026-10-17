// Request-driven API only: no heartbeat, watchdog, worker or timers. With CRON_SECRET set, an outside
// scheduler drives the pipeline in bounded batches (routes/pipeline.ts, jobs/batch.ts); without it this
// is a browsing-only deployment and collection and model calls must stay off.
import { assertProductionSecrets, config, credential } from "@aihot/backend/config";
import { installModules } from "@aihot/backend/modules";
import { SERVER_MODULES } from "@aihot/site/modules/server";
import { DEPLOYMENT } from "@aihot/site";
import { buildApp } from "./app.ts";

installModules(SERVER_MODULES);
const pipeline = !!credential("auth", "CRON_SECRET");
assertProductionSecrets([["auth", "SESSION_SECRET"], ["auth", "IMG_PROXY_SIGN_SECRET"], ...(pipeline ? [["auth", "CRON_SECRET"] as const] : []), ...DEPLOYMENT.requiredSecrets]);
if (!config.adminPassword || config.adminPassword.length < 12) throw new Error("ADMIN_PASSWORD must contain at least 12 characters");
if (!pipeline && (process.env.COLLECT_ENABLED === "true" || config.modelCallsEnabled)) {
  throw new Error("Collection and model calls need CRON_SECRET: without it this deployment is browsing-only");
}
const app = await buildApp();
await app.ready();
export default app.server;
