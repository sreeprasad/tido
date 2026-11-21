import { DurableObject } from "cloudflare:workers";
import type { Poll, Results, Snapshot, Tally } from "./room";

type Role = "audience" | "host";
type Attachment = { uid: string; role: Role };
type ShardMeta = { code: string; name: string };

const REPORT_DELAY_MS = 1000;
const MAX_MESSAGE_BYTES = 4096;

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

export class ShardDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Answer client keepalives at the edge without waking this object.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS votes (
        poll_id TEXT NOT NULL, round INTEGER NOT NULL, uid TEXT NOT NULL, options TEXT NOT NULL,
        PRIMARY KEY (poll_id, round, uid))`);
    });
  }

  // ---------- connection ----------

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const code = url.searchParams.get("code")!;
    const name = url.searchParams.get("shard")!;
    const uid = url.searchParams.get("uid")!;

    await this.ctx.storage.put<ShardMeta>("meta", { code, name });

    const [client, server] = Object.values(new WebSocketPair());
    const att: Attachment = { uid, role: "audience" };
    this.ctx.acceptWebSocket(server, [uid]);
    server.serializeAttachment(att);
    server.send(JSON.stringify({ type: "hello", uid, shard: name }));

    const snap = await this.env.ROOM.getByName(code).snapshot(name);
    await this.ctx.storage.put("poll", snap.poll);
    server.send(JSON.stringify(this.snapshotFor(snap, att)));

    await this.scheduleReport();
    return new Response(null, { status: 101, webSocket: client });
  }

  private filterResults(r: Results | null, role: Role): Results | null {
    if (!r) return null;
    return r.hidden && role !== "host" ? { ...r, counts: null } : r;
  }

  private yourVote(poll: Poll | null, uid: string): number[] | null {
    if (!poll) return null;
    const row = this.ctx.storage.sql
      .exec<{ options: string }>(
        `SELECT options FROM votes WHERE poll_id = ? AND round = ? AND uid = ?`,
        poll.id,
        poll.round,
        uid
      )
      .toArray()[0];
    return row ? (JSON.parse(row.options) as number[]) : null;
  }

  private snapshotFor(snap: Snapshot, att: Attachment) {
    return {
      type: "snapshot",
      role: att.role,
      poll: snap.poll,
      results: this.filterResults(snap.results, att.role),
      presence: snap.presence,
      yourVote: this.yourVote(snap.poll, att.uid),
    };
  }

  // ---------- reporting to the room ----------

  /** Coalesce changes: at most one report to the room per second. */
  private async scheduleReport() {
    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + REPORT_DELAY_MS);
    }
  }

  private tally(poll: Poll): Tally {
    const rows = this.ctx.storage.sql
      .exec<{ options: string }>(`SELECT options FROM votes WHERE poll_id = ? AND round = ?`, poll.id, poll.round)
      .toArray();
    const counts = new Array<number>(poll.options.length).fill(0);
    for (const r of rows) {
      for (const i of JSON.parse(r.options) as number[]) if (i >= 0 && i < counts.length) counts[i]++;
    }
    return { pollId: poll.id, round: poll.round, counts, voters: rows.length };
  }

  async alarm() {
    const meta = await this.ctx.storage.get<ShardMeta>("meta");
    if (!meta) return;
    const poll = (await this.ctx.storage.get<Poll | null>("poll")) ?? null;
    // Count unique people, not connections (one person may have several tabs open).
    const people = new Set(this.ctx.getWebSockets().map((w) => (w.deserializeAttachment() as Attachment).uid)).size;
    await this.env.ROOM.getByName(meta.code).report(meta.name, people, poll ? this.tally(poll) : null);
  }

  // ---------- messages from the room ----------

  /** Called by RoomDO to broadcast to every socket on this shard. */
  async push(msg: string) {
    const obj = JSON.parse(msg) as { type: string; poll?: Poll; hidden?: boolean; closed?: boolean };
    if (obj.type === "poll") await this.ctx.storage.put("poll", obj.poll ?? null);

    const audienceMsg = obj.type === "results" && obj.hidden ? JSON.stringify({ ...obj, counts: null }) : msg;
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment() as Attachment;
      try {
        ws.send(att.role === "host" ? msg : audienceMsg);
      } catch {}
    }

    // Room ended: disconnect everyone (clients stop reconnecting when they see this message).
    if (obj.type === "room" && obj.closed) {
      for (const ws of this.ctx.getWebSockets()) {
        try {
          ws.close(1000, "room closed");
        } catch {}
      }
    }
  }

  // ---------- messages from clients ----------

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    if (typeof message !== "string" || message.length > MAX_MESSAGE_BYTES) return;
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(message);
    } catch {
      return;
    }
    const att = ws.deserializeAttachment() as Attachment;

    if (msg.type === "auth") await this.handleAuth(ws, att, msg.token);
    else if (msg.type === "vote") await this.handleVote(ws, att, msg);
  }

  private async handleAuth(ws: WebSocket, att: Attachment, token: unknown) {
    if (typeof token !== "string" || !/^[a-f0-9]{64}$/.test(token)) return;
    const meta = await this.ctx.storage.get<ShardMeta>("meta");
    if (!meta) return;
    const room = this.env.ROOM.getByName(meta.code);
    if (!(await room.isHost(await sha256(token)))) {
      ws.send(JSON.stringify({ type: "error", error: "not host" }));
      return;
    }
    att.role = "host";
    ws.serializeAttachment(att);
    ws.send(JSON.stringify(this.snapshotFor(await room.snapshot(meta.name), att)));
  }

  private async handleVote(ws: WebSocket, att: Attachment, msg: Record<string, unknown>) {
    const poll = (await this.ctx.storage.get<Poll | null>("poll")) ?? null;
    const reject = (error: string) => ws.send(JSON.stringify({ type: "error", error }));

    if (!poll || poll.id !== msg.pollId) return reject("no such poll");
    if (poll.state !== "open") return reject("poll is not open");
    if (!Array.isArray(msg.options)) return reject("bad options");

    const options = [...new Set(msg.options)];
    const valid = options.every((i) => Number.isInteger(i) && (i as number) >= 0 && (i as number) < poll.options.length);
    if (!valid || options.length < 1 || (!poll.multi && options.length !== 1)) return reject("bad options");

    this.ctx.storage.sql.exec(
      `INSERT INTO votes (poll_id, round, uid, options) VALUES (?, ?, ?, ?)
       ON CONFLICT(poll_id, round, uid) DO UPDATE SET options = excluded.options`,
      poll.id,
      poll.round,
      att.uid,
      JSON.stringify(options)
    );
    // Tell every tab this person has open, so they all highlight the same choice.
    const voted = JSON.stringify({ type: "voted", pollId: poll.id, round: poll.round, options });
    for (const s of this.ctx.getWebSockets(att.uid)) {
      try {
        s.send(voted);
      } catch {}
    }
    await this.scheduleReport();
  }

  // ---------- disconnects ----------

  async webSocketClose(ws: WebSocket) {
    try {
      ws.close(1000, "bye");
    } catch {}
    await this.scheduleReport();
  }

  async webSocketError() {
    await this.scheduleReport();
  }
}
