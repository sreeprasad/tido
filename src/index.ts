import type { PastPoll } from "./room";

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

/** CSV cell: quote when needed, and neutralize spreadsheet formula injection (=, +, -, @). */
function csvCell(v: string | number): string {
  let s = String(v);
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function pollsToCsv(polls: PastPoll[]): string {
  const header = ["poll_number", "question", "state", "type", "option", "votes", "percent_of_voters", "voters", "created_at"];
  const lines = [header.join(",")];
  polls.forEach((p, n) => {
    p.poll.options.forEach((opt, i) => {
      const votes = p.counts[i] ?? 0;
      const pct = p.voters ? Math.round((1000 * votes) / p.voters) / 10 : 0;
      lines.push(
        [n + 1, p.poll.question, p.poll.state, p.poll.multi ? "multiple" : "single", opt, votes, pct, p.voters, p.createdAt]
          .map(csvCell)
          .join(",")
      );
    });
  });
  return lines.join("\r\n") + "\r\n";
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
      if (info.closed) return new Response("room closed", { status: 410 });

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

    // Room info (public)
    const infoMatch = path.match(/^\/api\/rooms\/([^/]+)$/);
    if (infoMatch && req.method === "GET") {
      const code = infoMatch[1].toLowerCase();
      if (!CODE_RE.test(code)) return Response.json({ error: "bad code" }, { status: 400 });
      const info = await env.ROOM.getByName(code).info();
      return info ? Response.json(info) : Response.json({ error: "not found" }, { status: 404 });
    }

    // Everything below is host-only: /api/rooms/:code/...
    const hostRoute = path.match(/^\/api\/rooms\/([^/]+)\/(.+)$/);
    if (!hostRoute) return new Response("not found", { status: 404 });
    const code = hostRoute[1].toLowerCase();
    const rest = hostRoute[2];
    const host = await isHost(req, env, code);
    const room = env.ROOM.getByName(code);

    // Verify host token
    if (rest === "host" && req.method === "POST") {
      return Response.json({ host }, { status: host ? 200 : 401 });
    }

    if (!host) return Response.json({ error: "unauthorized" }, { status: 401 });

    // List all polls with final results
    if (rest === "polls" && req.method === "GET") {
      return Response.json({ polls: await room.listPolls() });
    }

    // Create a poll
    if (rest === "polls" && req.method === "POST") {
      const body = await readJson(req);
      if (body === null) return Response.json({ error: "bad json" }, { status: 400 });
      const r = await room.createPoll(body);
      return r.ok ? Response.json({ poll: r.value }, { status: 201 }) : Response.json({ error: r.error }, { status: 400 });
    }

    // Open / close / reset a poll
    const action = rest.match(/^polls\/([a-f0-9]{8})\/(open|close|reset)$/);
    if (action && req.method === "POST") {
      const r = await room.setPollState(action[1], action[2] as "open" | "close" | "reset");
      return r.ok ? Response.json({ poll: r.value }) : Response.json({ error: r.error }, { status: 400 });
    }

    // Export all polls as CSV
    if (rest === "export.csv" && req.method === "GET") {
      const csv = pollsToCsv(await room.listPolls());
      return new Response(csv, {
        headers: {
          "content-type": "text/csv; charset=utf-8",
          "content-disposition": `attachment; filename="tido-${code}.csv"`,
          "cache-control": "no-store",
        },
      });
    }

    // Close the room
    if (rest === "close" && req.method === "POST") {
      const r = await room.closeRoom();
      return r.ok ? Response.json({ room: r.value }) : Response.json({ error: r.error }, { status: 400 });
    }

    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler<Env>;
