export interface Env {
  DB: D1Database;
  ASSETS: Fetcher;
  ENVIRONMENT?: string;
  APP_NAME?: string;
  INGEST_SECRET?: string;
}

export interface EventInput {
  id?: string;
  timestamp: string;
  event_type: string;
  asset?: string | null;
  indicator_type?: string | null;
  timeframe?: string | null;
  value?: number | null;
  percent_change?: number | null;
  direction?: string | null;
  source?: string | null;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface Event extends EventInput {
  id: string;
}

export interface SimilarityQuery {
  event_type: string;
  asset?: string;
  indicator_type?: string;
  timeframe?: string;
  direction?: string;
  tags?: string[];
  since?: string;
  until?: string;
  min_value?: number;
  max_value?: number;
}

export interface EventStats {
  total_events: number;
  earliest: string | null;
  latest: string | null;
  by_event_type: Record<string, number>;
  by_asset: Record<string, number>;
  monthly_counts: Record<string, number>;
  yearly_counts: Record<string, number>;
}

export interface TweetContextResponse {
  asset?: string | null;
  indicator_type?: string | null;
  event_type: string;
  current_value?: number | null;
  similar_events_since?: string | null;
  occurrences: number;
  percentile?: number | null;
  last_seen?: string | null;
  tweet_context: string;
  top_analogs: Event[];
}