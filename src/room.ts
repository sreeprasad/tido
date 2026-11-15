import { DurableObject } from "cloudflare:workers";

export class RoomDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(
        `CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)`
      );
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
    return true;
  }

  async info(): Promise<{ code: string; createdAt: string } | null> {
    const code = this.get("code");
    if (!code) return null;
    return { code, createdAt: this.get("created_at")! };
  }

  async isHost(tokenHash: string): Promise<boolean> {
    const stored = this.get("host_hash");
    return stored !== null && stored === tokenHash;
  }

  async ping(code: string) {
    return { code, exists: this.get("code") !== null };
  }
}
