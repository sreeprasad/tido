import { DurableObject } from "cloudflare:workers";

export class ShardDO extends DurableObject<Env> {
  async ping() {
    return { shardId: this.ctx.id.toString(), sockets: this.ctx.getWebSockets().length };
  }
}
