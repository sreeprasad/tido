import { DurableObject } from "cloudflare:workers";

export type Poll = {
  id: string;
  question: string;
  options: string[];
  multi: boolean;
  showResults: "live" | "after";
  state: "draft" | "open" | "closed";
  round: number;
};

export type Tally = { pollId: string; round: number; counts: number[]; voters: number };

export type Results = {
  type: "results";
  pollId: string;
  round: number;
  counts: number[] | null;
  voters: number;
  hidden: boolean;
};

export type Snapshot = { poll: Poll | null; results: Results | null; presence: number };

export type PastPoll = { poll: Poll; counts: number[]; voters: number; createdAt: string };

export type CloseReason = "host" | "expired" | "idle";

export type RoomInfo = {
  code: string;
  createdAt: string;
  shardCount: number;
  closed: boolean;
  closedAt: string | null;
  closedReason: CloseReason | null;
  expiresAt: string;
  maxExpiresAt: string;
  idleMinutes: number;
};

type Result<T> = { ok: true; value: T } | { ok: false; error: string };

const MIN = 60_000;
const EXTEND_MS = 30 * MIN;

export class RoomDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      const sql = this.ctx.storage.sql;
      sql.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)`);
      sql.exec(`CREATE TABLE IF NOT EXISTS shards (name TEXT PRIMARY KEY, sockets INTEGER NOT NULL)`);
      sql.exec(`CREATE TABLE IF NOT EXISTS polls (id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL)`);
      sql.exec(`CREATE TABLE IF NOT EXISTS shard_votes (
        poll_id TEXT NOT NULL, round INTEGER NOT NULL, shard TEXT NOT NULL,
        counts TEXT NOT NULL, voters INTEGER NOT NULL,
        PRIMARY KEY (poll_id, round, shard))`);
    });
  }

  // ---------- config ----------

  private cfg() {
    const n = (v: string | undefined, d: number) => (Number(v) > 0 ? Number(v) : d);
    return {
      idleMs: n(this.env.ROOM_IDLE_MINUTES, 10) * MIN,
      defaultMs: n(this.env.ROOM_DEFAULT_MINUTES, 60) * MIN,
      maxMs: n(this.env.ROOM_MAX_MINUTES, 180) * MIN,
    };
  }

  // ---------- small helpers ----------

  private get(key: string): string | null {
    const row = this.ctx.storage.sql
      .exec<{ value: string }>(`SELECT value FROM meta WHERE key = ?`, key)
      .toArray()[0];
    return row ? row.value : null;
  }

  private set(key: string, value: string) {
    this.ctx.storage.sql.exec(
      `INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
      key,
      value
    );
  }

  private isClosed(): boolean {
    return this.get("closed_at") !== null;
  }

  private createdMs(): number {
    return Date.parse(this.get("created_at") ?? new Date().toISOString());
  }

  private expiresMs(): number {
    const e = this.get("expires_at");
    return e ? Date.parse(e) : this.createdMs() + this.cfg().defaultMs;
  }

  private maxExpiresMs(): number {
    return this.createdMs() + this.cfg().maxMs;
  }

  /** When the room became empty (nobody connected), or null if someone is here. */
  private emptySinceMs(): number | null {
    const v = this.get("empty_since");
    if (v === "") return null;
    if (v === null) return this.presence() === 0 ? this.createdMs() : null; // rooms from before this feature
    return Date.parse(v);
  }

  private activePoll(): Poll | null {
    const id = this.get("active_poll");
    if (!id) return null;
    const row = this.ctx.storage.sql
      .exec<{ data: string }>(`SELECT data FROM polls WHERE id = ?`, id)
      .toArray()[0];
    return row ? (JSON.parse(row.data) as Poll) : null;
  }

  private savePoll(p: Poll) {
    this.ctx.storage.sql.exec(
      `INSERT INTO polls (id, data, created_at) VALUES (?, ?, ?)
       ON CONFLICT(id) DO UPDATE SET data = excluded.data`,
      p.id,
      JSON.stringify(p),
      new Date().toISOString()
    );
  }

  private totals(p: Poll): { counts: number[]; voters: number } {
    const rows = this.ctx.storage.sql
      .exec<{ counts: string; voters: number }>(
        `SELECT counts, voters FROM shard_votes WHERE poll_id = ? AND round = ?`,
        p.id,
        p.round
      )
      .toArray();
    const counts = new Array<number>(p.options.length).fill(0);
    let voters = 0;
    for (const r of rows) {
      const c = JSON.parse(r.counts) as number[];
      for (let i = 0; i < counts.length; i++) counts[i] += c[i] ?? 0;
      voters += r.voters;
    }
    return { counts, voters };
  }

  private results(p: Poll): Results {
    const { counts, voters } = this.totals(p);
    const hidden = p.showResults === "after" && p.state !== "closed";
    return { type: "results", pollId: p.id, round: p.round, counts, voters, hidden };
  }

  private presence(): number {
    return Math.max(0, Number(this.get("presence_total") ?? "0"));
  }

  private async fanout(msg: unknown) {
    const json = JSON.stringify(msg);
    const names = this.ctx.storage.sql
      .exec<{ name: string }>(`SELECT name FROM shards`)
      .toArray()
      .map((r) => r.name);
    await Promise.all(names.map((n) => this.env.SHARD.getByName(n).push(json).catch(() => {})));
  }

  private async broadcastResults(p: Poll) {
    const r = this.results(p);
    const key = JSON.stringify(r);
    if (key === this.get("last_results")) return;
    this.set("last_results", key);
    await this.fanout(r);
  }

  // ---------- auto-close ----------

  private dueReason(now = Date.now()): CloseReason | null {
    if (now >= this.expiresMs()) return "expired";
    const empty = this.emptySinceMs();
    if (empty !== null && now - empty >= this.cfg().idleMs) return "idle";
    return null;
  }

  /** Closes the room if it is past its deadline. Returns true if the room is (now) closed. */
  private async expireIfDue(): Promise<boolean> {
    if (!this.get("code")) return false;
    if (this.isClosed()) return true;
    const reason = this.dueReason();
    if (!reason) return false;
    await this.close(reason);
    return true;
  }

  /** Arm the alarm for the next deadline: expiry, or idle timeout if the room is empty. */
  private async scheduleAlarm() {
    if (this.isClosed() || !this.get("code")) return;
    let next = this.expiresMs();
    const empty = this.emptySinceMs();
    if (empty !== null) next = Math.min(next, empty + this.cfg().idleMs);
    await this.ctx.storage.setAlarm(next);
  }

  async alarm() {
    if (!(await this.expireIfDue())) await this.scheduleAlarm();
  }

  private async close(reason: CloseReason) {
    if (this.isClosed()) return;
    const p = this.activePoll();
    if (p && p.state !== "closed") {
      p.state = "closed";
      this.savePoll(p);
      await this.fanout({ type: "poll", poll: p });
      await this.broadcastResults(p);
    }
    this.set("closed_at", new Date().toISOString());
    this.set("closed_reason", reason);
    await this.ctx.storage.deleteAlarm();
    await this.fanout({ type: "room", closed: true, reason });
  }

  // ---------- room lifecycle ----------

  /** Returns false if this code is already taken. */
  async init(code: string, hostTokenHash: string): Promise<boolean> {
    if (this.get("code")) return false;
    const now = Date.now();
    this.set("code", code);
    this.set("host_hash", hostTokenHash);
    this.set("created_at", new Date(now).toISOString());
    this.set("expires_at", new Date(now + this.cfg().defaultMs).toISOString());
    this.set("empty_since", new Date(now).toISOString());
    this.set("shard_count", "1");
    await this.scheduleAlarm();
    return true;
  }

  async info(): Promise<RoomInfo | null> {
    const code = this.get("code");
    if (!code) return null;
    await this.expireIfDue();
    return {
      code,
      createdAt: this.get("created_at")!,
      shardCount: Number(this.get("shard_count") ?? "1"),
      closed: this.isClosed(),
      closedAt: this.get("closed_at"),
      closedReason: (this.get("closed_reason") as CloseReason | null) ?? (this.isClosed() ? "host" : null),
      expiresAt: new Date(this.expiresMs()).toISOString(),
      maxExpiresAt: new Date(this.maxExpiresMs()).toISOString(),
      idleMinutes: this.cfg().idleMs / MIN,
    };
  }

  async isHost(tokenHash: string): Promise<boolean> {
    const stored = this.get("host_hash");
    return stored !== null && stored === tokenHash;
  }

  /** Host ends the room. */
  async closeRoom(): Promise<Result<RoomInfo>> {
    await this.close("host");
    return { ok: true, value: (await this.info())! };
  }

  /** Host adds 30 minutes, up to the maximum room length. */
  async extend(): Promise<Result<RoomInfo>> {
    if (await this.expireIfDue()) return { ok: false, error: "room is closed" };
    const current = this.expiresMs();
    const max = this.maxExpiresMs();
    if (current >= max) return { ok: false, error: "room is already at the maximum length" };
    const next = Math.min(current + EXTEND_MS, max);
    this.set("expires_at", new Date(next).toISOString());
    await this.scheduleAlarm();
    await this.fanout({ type: "room", closed: false, expiresAt: new Date(next).toISOString() });
    return { ok: true, value: (await this.info())! };
  }

  /** Every poll in this room, oldest first, with its final counts (host only). */
  async listPolls(): Promise<PastPoll[]> {
    return this.ctx.storage.sql
      .exec<{ data: string; created_at: string }>(`SELECT data, created_at FROM polls ORDER BY created_at ASC`)
      .toArray()
      .map((row) => {
        const poll = JSON.parse(row.data) as Poll;
        return { poll, ...this.totals(poll), createdAt: row.created_at };
      });
  }

  // ---------- called by shards ----------

  /** Shard asks for current state on connect; also registers the shard for fan-out. */
  async snapshot(shardName: string): Promise<Snapshot> {
    this.ctx.storage.sql.exec(
      `INSERT INTO shards (name, sockets) VALUES (?, 0) ON CONFLICT(name) DO NOTHING`,
      shardName
    );
    const poll = this.activePoll();
    return { poll, results: poll ? this.results(poll) : null, presence: this.presence() };
  }

  /** Shard reports unique-people count and its absolute vote tally (idempotent). */
  async report(shardName: string, sockets: number, tally: Tally | null): Promise<void> {
    const sql = this.ctx.storage.sql;
    sql.exec(
      `INSERT INTO shards (name, sockets) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET sockets = excluded.sockets`,
      shardName,
      sockets
    );
    const total = sql.exec<{ t: number }>(`SELECT COALESCE(SUM(sockets), 0) AS t FROM shards`).one().t;
    const previous = Number(this.get("presence_total") ?? "-1");
    if (total !== previous) {
      this.set("presence_total", String(total));
      // Track emptiness for the idle timeout.
      if (total === 0) this.set("empty_since", new Date().toISOString());
      else this.set("empty_since", "");
      await this.scheduleAlarm();
      await this.fanout({ type: "presence", total });
    }

    const p = this.activePoll();
    if (tally && p && tally.pollId === p.id && tally.round === p.round) {
      sql.exec(
        `INSERT INTO shard_votes (poll_id, round, shard, counts, voters) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(poll_id, round, shard) DO UPDATE SET counts = excluded.counts, voters = excluded.voters`,
        p.id,
        p.round,
        shardName,
        JSON.stringify(tally.counts),
        tally.voters
      );
      await this.broadcastResults(p);
    }
  }

  // ---------- host actions ----------

  async createPoll(input: unknown): Promise<Result<Poll>> {
    if (await this.expireIfDue()) return { ok: false, error: "room is closed" };
    const i = (input ?? {}) as Record<string, unknown>;
    const question = typeof i.question === "string" ? i.question.trim() : "";
    if (question.length < 1 || question.length > 200) return { ok: false, error: "question must be 1–200 characters" };
    if (!Array.isArray(i.options)) return { ok: false, error: "options must be an array" };
    const options = i.options.map((o) => (typeof o === "string" ? o.trim() : ""));
    if (options.length < 2 || options.length > 6) return { ok: false, error: "need 2–6 options" };
    if (options.some((o) => o.length < 1 || o.length > 80)) return { ok: false, error: "each option must be 1–80 characters" };

    const poll: Poll = {
      id: crypto.randomUUID().slice(0, 8),
      question,
      options,
      multi: i.multi === true,
      showResults: i.showResults === "live" ? "live" : "after",
      state: "draft",
      round: 1,
    };
    // Starting a new poll closes the previous one if it was still open.
    const prev = this.activePoll();
    if (prev && prev.state === "open") {
      prev.state = "closed";
      this.savePoll(prev);
    }
    this.savePoll(poll);
    this.set("active_poll", poll.id);
    await this.fanout({ type: "poll", poll });
    await this.broadcastResults(poll);
    return { ok: true, value: poll };
  }

  async setPollState(id: string, action: "open" | "close" | "reset"): Promise<Result<Poll>> {
    if (await this.expireIfDue()) return { ok: false, error: "room is closed" };
    const p = this.activePoll();
    if (!p || p.id !== id) return { ok: false, error: "poll not found or not active" };
    if (action === "open") p.state = "open";
    if (action === "close") p.state = "closed";
    if (action === "reset") {
      this.ctx.storage.sql.exec(`DELETE FROM shard_votes WHERE poll_id = ?`, p.id);
      p.round += 1;
    }
    this.savePoll(p);
    await this.fanout({ type: "poll", poll: p });
    await this.broadcastResults(p);
    return { ok: true, value: p };
  }
}
