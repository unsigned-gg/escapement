// Pure request router — unit-testable without a Workers runtime.

export interface RouteResult {
  status: number;
  body: Record<string, unknown>;
}

export const SERVICE = "escapement-edge";
export const VERSION = "0.1.0";

export function route(method: string, pathname: string): RouteResult {
  if (method !== "GET") {
    return { status: 405, body: { error: "method not allowed" } };
  }
  switch (pathname) {
    case "/healthz":
      return { status: 200, body: { status: "ok" } };
    case "/readyz":
      // The edge has no engine binding yet; ready = the worker itself is up.
      return { status: 200, body: { status: "ok" } };
    case "/version":
      return { status: 200, body: { service: SERVICE, version: VERSION } };
    default:
      return { status: 404, body: { error: "not found" } };
  }
}
