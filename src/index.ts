import { Hono } from "hono";
import { cors } from "hono/cors";
import { parseTagsParam } from "./query";
import {
  ensureTables,
  findSimilar,
  getEvents,
  health,
  ingestEvents,
  latestSimilar,
  percentile,
  pruneBefore,
  pruneKeepMonths,
  stats,
  tweetContext,
  countSimilar,
} from "./storage";
import type { Env, EventInput, SimilarityQuery } from "./types";

const app = new Hono<{ Bindings: Env }>();

app.use("*", cors({ origin: "*" }));
app.use("*", async (c, next) => {
  await ensureTables(c.env.DB);
  await next();
});

function queryFromParams(q: Record<string, string | undefined>): SimilarityQuery {
  return {
    event_type: q.event_type ?? "",
    asset: q.asset,
    indicator_type: q.indicator_type,
    timeframe: q.timeframe,
    direction: q.direction,
    tags: parseTagsParam(q.tags),
    since: q.since,
    until: q.until,
    min_value: q.min_value != null ? parseFloat(q.min_value) : undefined,
    max_value: q.max_value != null ? parseFloat(q.max_value) : undefined,
  };
}

app.get("/health", async (c) => c.json(await health(c.env.DB)));

app.post("/ingest", async (c) => {
  const secret = c.env.INGEST_SECRET;
  if (secret) {
    const auth = c.req.header("Authorization") ?? "";
    if (auth !== `Bearer ${secret}`) return c.json({ detail: "Unauthorized" }, 401);
  }
  const body = await c.req.json<{ events: EventInput[] }>();
  const count = await ingestEvents(c.env.DB, body.events ?? []);
  return c.json({ ingested: count, status: "ok" });
});

app.get("/events", async (c) => {
  const q = c.req.query();
  const events = await getEvents(c.env.DB, {
    event_type: q.event_type,
    asset: q.asset,
    indicator_type: q.indicator_type,
    since: q.since,
    until: q.until,
    limit: Math.min(500, Math.max(1, parseInt(q.limit ?? "50", 10))),
    offset: Math.max(0, parseInt(q.offset ?? "0", 10)),
  });
  return c.json({ events, count: events.length });
});

app.get("/similar", async (c) => {
  const q = queryFromParams(c.req.query());
  if (!q.event_type) return c.json({ detail: "event_type is required" }, 400);
  const limit = Math.min(200, Math.max(1, parseInt(c.req.query("limit") ?? "20", 10)));
  const events = await findSimilar(c.env.DB, q, limit);
  return c.json({ events, count: events.length });
});

app.get("/count", async (c) => {
  const q = queryFromParams(c.req.query());
  if (!q.event_type) return c.json({ detail: "event_type is required" }, 400);
  return c.json({ count: await countSimilar(c.env.DB, q), query: q });
});

app.get("/latest", async (c) => {
  const q = queryFromParams(c.req.query());
  if (!q.event_type) return c.json({ detail: "event_type is required" }, 400);
  const event = await latestSimilar(c.env.DB, q);
  if (!event) return c.json({ detail: "No matching event found" }, 404);
  return c.json(event);
});

app.get("/percentile", async (c) => {
  const q = queryFromParams(c.req.query());
  const currentValue = parseFloat(c.req.query("current_value") ?? "");
  if (!q.event_type || Number.isNaN(currentValue)) {
    return c.json({ detail: "event_type and current_value are required" }, 400);
  }
  const value = await percentile(c.env.DB, currentValue, q);
  if (value == null) return c.json({ detail: "Not enough data for percentile" }, 404);
  return c.json({ percentile: value, current_value: currentValue });
});

app.get("/tweet-context", async (c) => {
  const q = queryFromParams(c.req.query());
  if (!q.event_type) return c.json({ detail: "event_type is required" }, 400);
  const currentRaw = c.req.query("current_value");
  const currentValue = currentRaw != null ? parseFloat(currentRaw) : undefined;
  const analogLimit = Math.min(10, Math.max(0, parseInt(c.req.query("analog_limit") ?? "3", 10)));
  return c.json(
    await tweetContext(
      c.env.DB,
      q,
      currentValue != null && !Number.isNaN(currentValue) ? currentValue : undefined,
      analogLimit,
    ),
  );
});

app.get("/stats", async (c) => c.json(await stats(c.env.DB)));

app.post("/prune", async (c) => {
  const before = c.req.query("before");
  const keepMonthsRaw = c.req.query("keep_months");
  if (before && keepMonthsRaw) return c.json({ detail: "Use either before or keep_months" }, 400);
  if (before) {
    const deleted = await pruneBefore(c.env.DB, before);
    return c.json({ deleted, status: "ok" });
  }
  if (keepMonthsRaw) {
    const months = parseInt(keepMonthsRaw, 10);
    if (Number.isNaN(months) || months < 1) return c.json({ detail: "Invalid keep_months" }, 400);
    const deleted = await pruneKeepMonths(c.env.DB, months);
    return c.json({ deleted, status: "ok" });
  }
  return c.json({ detail: "Provide before or keep_months" }, 400);
});

export default app;