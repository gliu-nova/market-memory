import type { SimilarityQuery } from "./types";

export function buildWhere(query: Partial<SimilarityQuery>): { sql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (query.event_type) {
    clauses.push("event_type = ?");
    params.push(query.event_type);
  }
  if (query.asset) {
    clauses.push("asset = ?");
    params.push(query.asset);
  }
  if (query.indicator_type) {
    clauses.push("indicator_type = ?");
    params.push(query.indicator_type);
  }
  if (query.timeframe) {
    clauses.push("timeframe = ?");
    params.push(query.timeframe);
  }
  if (query.direction) {
    clauses.push("direction = ?");
    params.push(query.direction);
  }
  if (query.since) {
    clauses.push("timestamp >= ?");
    params.push(normalizeTimestamp(query.since));
  }
  if (query.until) {
    clauses.push("timestamp <= ?");
    params.push(normalizeTimestamp(query.until));
  }
  if (query.min_value != null) {
    clauses.push("value >= ?");
    params.push(query.min_value);
  }
  if (query.max_value != null) {
    clauses.push("value <= ?");
    params.push(query.max_value);
  }
  if (query.tags?.length) {
    const tagChecks = query.tags.map(() => {
      return "EXISTS (SELECT 1 FROM json_each(tags) je WHERE je.value = ?)";
    });
    clauses.push(`(${tagChecks.join(" OR ")})`);
    params.push(...query.tags);
  }

  return {
    sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    params,
  };
}

export function normalizeTimestamp(value: string): string {
  return value.replace("Z", "").replace(/\+00:00$/, "");
}

export function parseTagsParam(tags?: string | null): string[] {
  if (!tags) return [];
  return tags.split(",").map((t) => t.trim()).filter(Boolean);
}