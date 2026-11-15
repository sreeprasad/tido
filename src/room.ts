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

  async ping(code: string) {
    const sql = this.ctx.storage.sql;
    sql.exec(`INSERT INTO meta (key, value) VALUES ('code', ?) ON CONFLICT(key) DO NOTHING`, code);
    sql.exec(`INSERT INTO meta (key, value) VALUES ('pings', '0') ON CONFLICT(key) DO NOTHING`);
    sql.exec(`UPDATE meta SET value = CAST(value AS INTEGER) + 1 WHERE key = 'pings'`);
    const rows = sql.exec<{ key: string; value: string }>(`SELECT key, value FROM meta`).toArray();
    return Object.fromEntries(rows.map((r) => [r.key, r.value]));
  }
}
