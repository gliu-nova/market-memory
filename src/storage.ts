import { buildTweetContext } from "./context";
import { buildWhere, normalizeTimestamp } from "./query";
import type { Event, EventInput, EventStats, SimilarityQuery, TweetContextResponse } from "./types";

type EventRow = {
  id: string;
  timestamp: string;
  event_type: string;
  asset: string | null;
  indicator_type: string | null;
  timeframe: string | null;
  value: number | null;
  percent_change: number | null;
  direction: string | null;
  source: string | null;
  tags: string;
  metadata: string;
};

export async function ensureTables(db: D1Database): Promise<void> {
  await db.batch([
    db.prepare(`CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      timestamp TEXT NOT NULL,
      event_type TEXT NOT NULL,
      asset TEXT,
      indicator_type TEXT,
      timeframe TEXT,
      value REAL,
      percent_change REAL,
      direction TEXT,
      source TEXT,
      tags TEXT NOT NULL DEFAULT '[]',
      metadata TEXT NOT NULL DEFAULT '{}'
    )`),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)"),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp)"),
    db.prepare("CREATE INDEX IF NOT EXISTS idx_events_asset_indicator ON events(asset, indicator_type, timestamp)"),
  ]);
}

function rowToEvent(row: EventRow): Event {
  return {
    id: row.id,
    timestamp: row.timestamp,
    event_type: row.event_type,
    asset: row.asset,
    indicator_type: row.indicator_type,
    timeframe: row.timeframe,
    value: row.value,
    percent_change: row.percent_change,
    direction: row.direction,
    source: row.source,
    tags: JSON.parse(row.tags || "[]"),
    metadata: JSON.parse(row.metadata || "{}"),
  };
}

function newId(): string {
  return crypto.randomUUID();
}

export async function ingestEvents(db: D1Database, events: EventInput[]): Promise<number> {
  if (!events.length) return 0;
  const stmts = events.map((event) => {
    const id = event.id ?? newId();
    return db
      .prepare(
        `INSERT OR REPLACE INTO events
         (id, timestamp, event_type, asset, indicator_type, timeframe,
          value, percent_change, direction, source, tags, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        id,
        normalizeTimestamp(event.timestamp),
        event.event_type,
        event.asset ?? null,
        event.indicator_type ?? null,
        event.timeframe ?? null,
        event.value ?? null,
        event.percent_change ?? null,
        event.direction ?? null,
        event.source ?? null,
        JSON.stringify(event.tags ?? []),
        JSON.stringify(event.metadata ?? {}),
      );
  });
  await db.batch(stmts);
  return events.length;
}

export async function getEvents(
  db: D1Database,
  filters: Partial<SimilarityQuery> & { limit?: number; offset?: number },
): Promise<Event[]> {
  const { sql, params } = buildWhere(filters);
  const limit = filters.limit ?? 100;
  const offset = filters.offset ?? 0;
  const rows = await db
    .prepare(`SELECT * FROM events ${sql} ORDER BY timestamp DESC LIMIT ? OFFSET ?`)
    .bind(...params, limit, offset)
    .all<EventRow>();
  return (rows.results ?? []).map(rowToEvent);
}

export async function findSimilar(db: D1Database, query: SimilarityQuery, limit = 50): Promise<Event[]> {
  const { sql, params } = buildWhere(query);
  const rows = await db
    .prepare(`SELECT * FROM events ${sql} ORDER BY timestamp DESC LIMIT ?`)
    .bind(...params, limit)
    .all<EventRow>();
  return (rows.results ?? []).map(rowToEvent);
}

export async function countSimilar(db: D1Database, query: SimilarityQuery): Promise<number> {
  const { sql, params } = buildWhere(query);
  const row = await db.prepare(`SELECT COUNT(*) AS c FROM events ${sql}`).bind(...params).first<{ c: number }>();
  return row?.c ?? 0;
}

export async function latestSimilar(db: D1Database, query: SimilarityQuery): Promise<Event | null> {
  const events = await findSimilar(db, query, 1);
  return events[0] ?? null;
}

export async function percentile(db: D1Database, currentValue: number, query: SimilarityQuery): Promise<number | null> {
  const { sql, params } = buildWhere(query);
  const row = await db
    .prepare(
      `SELECT COUNT(*) AS total,
              SUM(CASE WHEN value < ? THEN 1 ELSE 0 END) AS below
       FROM events ${sql} AND value IS NOT NULL`,
    )
    .bind(currentValue, ...params)
    .first<{ total: number; below: number }>();
  if (!row || row.total === 0) return null;
  return Math.round((100.0 * row.below) / row.total * 10) / 10;
}

export async function topAnalogs(
  db: D1Database,
  currentValue: number,
  query: SimilarityQuery,
  limit = 5,
): Promise<Event[]> {
  const { sql, params } = buildWhere(query);
  const rows = await db
    .prepare(
      `SELECT * FROM events ${sql} AND value IS NOT NULL
       ORDER BY ABS(value - ?) ASC, timestamp DESC LIMIT ?`,
    )
    .bind(...params, currentValue, limit)
    .all<EventRow>();
  return (rows.results ?? []).map(rowToEvent);
}

export async function stats(db: D1Database): Promise<EventStats> {
  const totalRow = await db
    .prepare("SELECT COUNT(*) AS total, MIN(timestamp) AS earliest, MAX(timestamp) AS latest FROM events")
    .first<{ total: number; earliest: string | null; latest: string | null }>();

  const byType = await db
    .prepare("SELECT event_type, COUNT(*) AS c FROM events GROUP BY event_type ORDER BY c DESC")
    .all<{ event_type: string; c: number }>();
  const byAsset = await db
    .prepare("SELECT asset, COUNT(*) AS c FROM events WHERE asset IS NOT NULL GROUP BY asset ORDER BY c DESC")
    .all<{ asset: string; c: number }>();
  const monthly = await db
    .prepare("SELECT strftime('%Y-%m', timestamp) AS period, COUNT(*) AS c FROM events GROUP BY period ORDER BY period")
    .all<{ period: string; c: number }>();
  const yearly = await db
    .prepare("SELECT strftime('%Y', timestamp) AS period, COUNT(*) AS c FROM events GROUP BY period ORDER BY period")
    .all<{ period: string; c: number }>();

  const toMap = (rows: { period?: string; event_type?: string; asset?: string; c: number }[], key: "period" | "event_type" | "asset") =>
    Object.fromEntries(rows.map((r) => [r[key] as string, r.c]));

  return {
    total_events: totalRow?.total ?? 0,
    earliest: totalRow?.earliest ?? null,
    latest: totalRow?.latest ?? null,
    by_event_type: toMap(byType.results ?? [], "event_type"),
    by_asset: toMap(byAsset.results ?? [], "asset"),
    monthly_counts: toMap(monthly.results ?? [], "period"),
    yearly_counts: toMap(yearly.results ?? [], "period"),
  };
}

export async function pruneBefore(db: D1Database, before: string): Promise<number> {
  const beforeCount = await db.prepare("SELECT COUNT(*) AS c FROM events").first<{ c: number }>();
  await db.prepare("DELETE FROM events WHERE timestamp < ?").bind(normalizeTimestamp(before)).run();
  const afterCount = await db.prepare("SELECT COUNT(*) AS c FROM events").first<{ c: number }>();
  return (beforeCount?.c ?? 0) - (afterCount?.c ?? 0);
}

export async function pruneKeepMonths(db: D1Database, months: number): Promise<number> {
  const countRow = await db
    .prepare(
      `SELECT COUNT(*) AS c FROM events
       WHERE timestamp < datetime((SELECT MAX(timestamp) FROM events), ?)`,
    )
    .bind(`-${months} months`)
    .first<{ c: number }>();
  const toDelete = countRow?.c ?? 0;
  if (toDelete > 0) {
    await db
      .prepare(
        `DELETE FROM events
         WHERE timestamp < datetime((SELECT MAX(timestamp) FROM events), ?)`,
      )
      .bind(`-${months} months`)
      .run();
  }
  return toDelete;
}

export async function tweetContext(
  db: D1Database,
  query: SimilarityQuery,
  currentValue?: number | null,
  analogLimit = 3,
): Promise<TweetContextResponse> {
  const occurrences = await countSimilar(db, query);
  const latest = await latestSimilar(db, query);
  const pct = currentValue != null ? await percentile(db, currentValue, query) : null;
  const analogs =
    currentValue != null
      ? await topAnalogs(db, currentValue, query, analogLimit)
      : await findSimilar(db, query, analogLimit);

  return buildTweetContext({
    event_type: query.event_type,
    asset: query.asset,
    indicator_type: query.indicator_type,
    direction: query.direction,
    current_value: currentValue,
    since: query.since,
    occurrences,
    percentile: pct,
    last_seen: latest?.timestamp ?? null,
    top_analogs: analogs,
  });
}

export async function health(db: D1Database): Promise<Record<string, unknown>> {
  const summary = await stats(db);
  return {
    status: "ok",
    runtime: "cloudflare-pages",
    total_events: summary.total_events,
    earliest: summary.earliest,
    latest: summary.latest,
  };
}