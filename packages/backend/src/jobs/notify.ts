// Content pushes: newly selected items, and the images they need prepared first.
import type { PgBoss } from "pg-boss";
import { pushSelected } from "../notify/selected.ts";
import { prepareArticleMedia, warmShareImage } from "../media/prepare.ts";
import { convertAnimated } from "../media/images.ts";
import { enqueue, handler, QUEUES, workHandlers } from "./queue.ts";

const MAX_RETRIES = 6;

export const notifyHandlers = () => [
  handler(QUEUES.notifySelected, 1, 5, async ({ articleId, attempt = 0 }) => {
    // The push makes chat apps unfurl the link: have its share image ready (first attempt only).
    if (!attempt) await warmShareImage(articleId);
    const outcome = await pushSelected(articleId);
    if (outcome.status === "retry" && attempt < MAX_RETRIES) {
      await enqueue(QUEUES.notifySelected, { articleId, attempt: attempt + 1 }, { startAfter: outcome.after });
    }
    return outcome;
  }),
  handler(QUEUES.prepareMedia, 1, 5, async (data) =>
    "articleId" in data ? prepareArticleMedia(data.articleId) : { animatedSaved: await convertAnimated(data.url, data.mode) }),
];

export async function registerNotifyJobs(boss: PgBoss) {
  await workHandlers(boss, notifyHandlers());
}
