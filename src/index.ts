export { RoomDO } from "./room";
export { ShardDO } from "./shard";

const CODE_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz";
const CODE_RE = /^[23456789abcdefghjkmnpqrstuvwxyz]{6}$/;

function randomCode(len = 6): string {
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return Array.from(bytes, (b) => CODE_ALPHABET[b % CODE_ALPHABET.length]).join("");
}

function randomToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

function bearer(req: Request): string | null {
  const h = req.headers.get("authorization") ?? "";
  const m = h.match(/^Bearer ([a-f0-9]{64})$/);
  return m ? m[1] : null;
}

export default {
  async fetch(req, env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    // Create a room
    if (path === "/api/rooms" && req.method === "POST") {
      for (let attempt = 0; attempt < 5; attempt++) {
        const code = randomCode();
        const token = randomToken();
        const created = await env.ROOM.getByName(code).init(code, await sha256(token));
        if (created) return Response.json({ code, hostToken: token }, { status: 201 });
      }
      return Response.json({ error: "could not allocate a room code" }, { status: 503 });
    }

    // Room info
    const infoMatch = path.match(/^\/api\/rooms\/([^/]+)$/);
    if (infoMatch && req.method === "GET") {
      const code = infoMatch[1].toLowerCase();
      if (!CODE_RE.test(code)) return Response.json({ error: "bad code" }, { status: 400 });
      const info = await env.ROOM.getByName(code).info();
      return info ? Response.json(info) : Response.json({ error: "not found" }, { status: 404 });
    }

    // Verify host token
    const hostMatch = path.match(/^\/api\/rooms\/([^/]+)\/host$/);
    if (hostMatch && req.method === "POST") {
      const code = hostMatch[1].toLowerCase();
      const token = bearer(req);
      if (!CODE_RE.test(code) || !token) return Response.json({ host: false }, { status: 401 });
      const ok = await env.ROOM.getByName(code).isHost(await sha256(token));
      return Response.json({ host: ok }, { status: ok ? 200 : 401 });
    }

    return new Response("Tido", { headers: { "content-type": "text/plain" } });
  },
} satisfies ExportedHandler<Env>;
