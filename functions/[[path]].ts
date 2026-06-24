import { handle } from "hono/cloudflare-pages";
import app from "../src/index";

const hono = handle(app);

export const onRequest = async (context: {
  request: Request;
  env: { DB: D1Database; ASSETS: { fetch: (req: Request) => Promise<Response> } };
  waitUntil: (p: Promise<unknown>) => void;
  passThroughOnException: () => void;
  next: () => Promise<Response>;
  params: Record<string, string>;
  data: Record<string, unknown>;
}) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response = await hono(context as any);
  if (response.status !== 404) {
    return response;
  }
  return context.env.ASSETS.fetch(context.request);
};