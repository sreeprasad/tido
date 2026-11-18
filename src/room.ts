import { DurableObject } from "cloudflare:workers";

export class RoomDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      const sql = this.ctx.storage.sql;
      sql.exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)`);
      sql.exec(`CREATE TABLE IF NOT EXISTS shards (name TEXT PRIMARY KEY, sockets INTEGER NOT NULL)`);
    });
  }

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

  /** Returns false if this code is already taken. */
  async init(code: string, hostTokenHash: string): Promise<boolean> {
    if (this.get("code")) return false;
    this.set("code", code);
    this.set("host_hash", hostTokenHash);
    this.set("created_at", new Date().toISOString());
    this.set("shard_count", "1");
    return true;
  }

  async info(): Promise<{ code: string; createdAt: string; shardCount: number } | null> {
    const code = this.get("code");
    if (!code) return null;
    return {
      code,
      createdAt: this.get("created_at")!,
      shardCount: Number(this.get("shard_count") ?? "1"),
    };
  }

  async isHost(tokenHash: string): Promise<boolean> {
    const stored = this.get("host_hash");
    return stored !== null && stored === tokenHash;
  }

  /** A shard reports its socket count; if the room total changed, fan it out to every shard. */
  async report(shardName: string, sockets: number): Promise<{ total: number }> {
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
      const names = sql.exec<{ name: string }>(`SELECT name FROM shards`).toArray().map((r) => r.name);
      const msg = JSON.stringify({ type: "presence", total });
      await Promise.all(names.map((n) => this.env.SHARD.getByName(n).push(msg).catch(() => {})));
    }
    return { total };
  }
}
