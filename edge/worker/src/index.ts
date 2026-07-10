import { route } from "./router";

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const { status, body } = route(request.method, url.pathname);
    return new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
  },
} satisfies ExportedHandler;
