export { RoomDO } from "./room";
export { ShardDO } from "./shard";

const CODE_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz";
const CODE_RE = /^[23456789abcdefghjkmnpqrstuvwxyz]{6}$/;
const UID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const MAX_BODY_BYTES = 8192;

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

/** FNV-1a: stable, fast hash so a user always lands on the same shard. */
function fnv1a(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function bearer(req: Request): string | null {
  const h = req.headers.get("authorization") ?? "";
  const m = h.match(/^Bearer ([a-f0-9]{64})$/);
  return m ? m[1] : null;
}

async function isHost(req: Request, env: Env, code: string): Promise<boolean> {
  const token = bearer(req);
  if (!CODE_RE.test(code) || !token) return false;
  return env.ROOM.getByName(code).isHost(await sha256(token));
}

async function readJson(req: Request): Promise<unknown> {
  const len = Number(req.headers.get("content-length") ?? "0");
  if (len > MAX_BODY_BYTES) return null;
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

export default {
  async fetch(req, env): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    // WebSocket: route to a shard
    const wsMatch = path.match(/^\/ws\/([^/]+)$/);
    if (wsMatch) {
      if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      const code = wsMatch[1].toLowerCase();
      const uid = (url.searchParams.get("uid") ?? "").toLowerCase();
      if (!CODE_RE.test(code) || !UID_RE.test(uid)) return new Response("bad request", { status: 400 });

      const info = await env.ROOM.getByName(code).info();
      if (!info) return new Response("room not found", { status: 404 });

      const shardName = `${code}:${fnv1a(uid) % info.shardCount}`;
      const fwd = new URL(req.url);
      fwd.search = "";
      fwd.searchParams.set("code", code);
      fwd.searchParams.set("shard", shardName);
      fwd.searchParams.set("uid", uid);
      return env.SHARD.getByName(shardName).fetch(new Request(fwd, req));
    }

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
      const ok = await isHost(req, env, code);
      return Response.json({ host: ok }, { status: ok ? 200 : 401 });
    }

    // Host: create a poll
    const createPoll = path.match(/^\/api\/rooms\/([^/]+)\/polls$/);
    if (createPoll && req.method === "POST") {
      const code = createPoll[1].toLowerCase();
      if (!(await isHost(req, env, code))) return Response.json({ error: "unauthorized" }, { status: 401 });
      const body = await readJson(req);
      if (body === null) return Response.json({ error: "bad json" }, { status: 400 });
      const r = await env.ROOM.getByName(code).createPoll(body);
      return r.ok ? Response.json({ poll: r.value }, { status: 201 }) : Response.json({ error: r.error }, { status: 400 });
    }

    // Host: open / close / reset a poll
    const pollAction = path.match(/^\/api\/rooms\/([^/]+)\/polls\/([a-f0-9]{8})\/(open|close|reset)$/);
    if (pollAction && req.method === "POST") {
      const code = pollAction[1].toLowerCase();
      if (!(await isHost(req, env, code))) return Response.json({ error: "unauthorized" }, { status: 401 });
      const action = pollAction[3] as "open" | "close" | "reset";
      const r = await env.ROOM.getByName(code).setPollState(pollAction[2], action);
      return r.ok ? Response.json({ poll: r.value }) : Response.json({ error: r.error }, { status: 400 });
    }

    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
