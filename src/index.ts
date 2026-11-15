export { RoomDO } from "./room";
export { ShardDO } from "./shard";

export default {
  async fetch(req, env): Promise<Response> {
    const url = new URL(req.url);

    const ping = url.pathname.match(/^\/ping\/([a-z0-9]{1,12})$/i);
    if (ping) {
      const code = ping[1].toLowerCase();
      const room = env.ROOM.getByName(code);
      const shard = env.SHARD.getByName(`${code}:0`);
      const [roomState, shardState] = await Promise.all([room.ping(code), shard.ping()]);
      return Response.json({ room: roomState, shard: shardState });
    }

    return new Response("Tido", { headers: { "content-type": "text/plain" } });
  },
} satisfies ExportedHandler<Env>;
