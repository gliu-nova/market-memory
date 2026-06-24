import type { Event, TweetContextResponse } from "./types";

function ordinalPercentile(value: number): string {
  const rounded = Math.round(value);
  if (rounded % 100 >= 11 && rounded % 100 <= 13) return `${rounded}th`;
  const suffix = { 1: "st", 2: "nd", 3: "rd" }[rounded % 10] ?? "th";
  return `${rounded}${suffix}`;
}

function label(asset?: string | null, indicator?: string | null, direction?: string | null): string {
  const parts = [asset, indicator, direction].filter(Boolean);
  return parts.length ? parts.join(" ") : "similar events";
}

export function buildTweetContext(args: {
  event_type: string;
  asset?: string | null;
  indicator_type?: string | null;
  direction?: string | null;
  current_value?: number | null;
  since?: string | null;
  occurrences: number;
  percentile?: number | null;
  last_seen?: string | null;
  top_analogs?: Event[];
}): TweetContextResponse {
  const sinceLabel = args.since ? args.since.slice(0, 10) : null;
  const sincePhrase = sinceLabel ? ` since ${sinceLabel}` : "";
  const suffix = args.occurrences === 1 ? "occurrence" : "occurrences";

  const parts = [`Similar ${label(args.asset, args.indicator_type, args.direction)} events${sincePhrase}: ${args.occurrences} ${suffix}.`];

  if (args.percentile != null) {
    parts.push(`Current reading ranks in the ${ordinalPercentile(args.percentile)} percentile.`);
  }
  if (args.last_seen) {
    parts.push(`Last seen ${args.last_seen.slice(0, 10)}.`);
  }

  return {
    asset: args.asset,
    indicator_type: args.indicator_type,
    event_type: args.event_type,
    current_value: args.current_value,
    similar_events_since: sinceLabel,
    occurrences: args.occurrences,
    percentile: args.percentile,
    last_seen: args.last_seen ? args.last_seen.slice(0, 10) : null,
    tweet_context: parts.join(" "),
    top_analogs: args.top_analogs ?? [],
  };
}