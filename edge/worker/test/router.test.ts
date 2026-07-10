import { describe, expect, it } from "vitest";
import { route, SERVICE, VERSION } from "../src/router";

describe("route", () => {
  it("serves /healthz", () => {
    expect(route("GET", "/healthz")).toEqual({
      status: 200,
      body: { status: "ok" },
    });
  });

  it("serves /readyz", () => {
    expect(route("GET", "/readyz").status).toBe(200);
  });

  it("reports service identity on /version", () => {
    expect(route("GET", "/version")).toEqual({
      status: 200,
      body: { service: SERVICE, version: VERSION },
    });
  });

  it("404s unknown paths", () => {
    expect(route("GET", "/nope").status).toBe(404);
  });

  it("405s non-GET methods", () => {
    expect(route("POST", "/healthz").status).toBe(405);
  });
});
