// Request-driven API only: no heartbeat, watchdog, worker or scheduled jobs.
import { assertProductionSecrets, config } from "@aihot/backend/config";
import { installModules } from "@aihot/backend/modules";
import { SERVER_MODULES } from "@aihot/site/modules/server";
import { DEPLOYMENT } from "@aihot/site";
import { buildApp } from "./app.ts";

installModules(SERVER_MODULES);
assertProductionSecrets([["auth", "SESSION_SECRET"], ["auth", "IMG_PROXY_SIGN_SECRET"], ...DEPLOYMENT.requiredSecrets]);
if (!config.adminPassword || config.adminPassword.length < 12) throw new Error("ADMIN_PASSWORD must contain at least 12 characters");
if (process.env.COLLECT_ENABLED === "true" || config.modelCallsEnabled) throw new Error("This browsing deployment requires collection and model calls to stay disabled");
const app = await buildApp();
await app.ready();
export default app.server;
