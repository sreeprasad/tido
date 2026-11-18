import { DurableObject } from "cloudflare:workers";

type Attachment = { uid: string };
type ShardMeta = { code: string; name: string };

const REPORT_DELAY_MS = 1000;

export class ShardDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Answer client keepalives at the edge without waking this object.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const code = url.searchParams.get("code")!;
    const name = url.searchParams.get("shard")!;
    const uid = url.searchParams.get("uid")!;

    await this.ctx.storage.put<ShardMeta>("meta", { code, name });

    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, [uid]);
    server.serializeAttachment({ uid } satisfies Attachment);
    server.send(JSON.stringify({ type: "hello", uid, shard: name }));

    await this.scheduleReport();
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Coalesce joins/leaves: at most one report to the room per second. */
  private async scheduleReport() {
    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + REPORT_DELAY_MS);
    }
  }

  async alarm() {
    const meta = await this.ctx.storage.get<ShardMeta>("meta");
    if (!meta) return;
    const sockets = this.ctx.getWebSockets().length;
    await this.env.ROOM.getByName(meta.code).report(meta.name, sockets);
  }

  /** Called by RoomDO to broadcast a message to every socket on this shard. */
  async push(msg: string) {
    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(msg);
      } catch {}
    }
  }

  async webSocketMessage(_ws: WebSocket, _message: string | ArrayBuffer) {
    // Votes and questions arrive here in M4–M5.
  }

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
