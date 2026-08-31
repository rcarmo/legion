import type { Socket } from "bun";

const TVERSION = 100, RVERSION = 101, TATTACH = 104, RATTACH = 105;
const TWALK = 110, RWALK = 111, TREAD = 116, RREAD = 117, TWRITE = 118, RWRITE = 119, RLERROR = 7;
const NOTAG = 0xffff, NOFID = 0xffffffff, MSIZE = 65536;

export interface LegionFsOptions {
  hostname?: string;
  port?: number;
  capability?: string;
}

type Reply = { type: number; tag: number; body: Uint8Array };

class Writer {
  parts: Uint8Array[] = [];
  u16(value: number) { const b = new Uint8Array(2); new DataView(b.buffer).setUint16(0, value, true); this.parts.push(b); }
  u32(value: number) { const b = new Uint8Array(4); new DataView(b.buffer).setUint32(0, value, true); this.parts.push(b); }
  u64(value: number) { const b = new Uint8Array(8); new DataView(b.buffer).setBigUint64(0, BigInt(value), true); this.parts.push(b); }
  string(value: string) { const b = new TextEncoder().encode(value); this.u16(b.length); this.parts.push(b); }
  data(value: Uint8Array) { this.u32(value.length); this.parts.push(value); }
  finish(type: number, tag: number) {
    const length = 7 + this.parts.reduce((sum, part) => sum + part.length, 0);
    const output = new Uint8Array(length);
    const view = new DataView(output.buffer);
    view.setUint32(0, length, true); output[4] = type; view.setUint16(5, tag, true);
    let offset = 7;
    for (const part of this.parts) { output.set(part, offset); offset += part.length; }
    return output;
  }
}

/** Native Bun 9P2000.L client projected as a small fs/promises-compatible API. */
export class LegionFs {
  private socket?: Socket<undefined>;
  private buffer = new Uint8Array();
  private nextTag = 1;
  private nextFid = 1;
  private pending = new Map<number, { resolve: (reply: Reply) => void; reject: (error: Error) => void }>();

  constructor(private readonly options: LegionFsOptions = {}) {}

  async connect() {
    if (this.socket) return;
    this.socket = await Bun.connect({
      hostname: this.options.hostname ?? "127.0.0.1",
      port: this.options.port ?? 5640,
      socket: {
        data: (_socket, data) => this.onData(new Uint8Array(data)),
        error: (_socket, error) => this.fail(error),
        close: () => this.fail(new Error("9P connection closed")),
      },
    });
    const version = new Writer(); version.u32(MSIZE); version.string("9P2000.L");
    await this.rpc(TVERSION, RVERSION, version, NOTAG);
  }

  close() { this.socket?.end(); this.socket = undefined; }

  async readFile(path: string, encoding?: "utf8"): Promise<Uint8Array | string> {
    await this.connect();
    const fid = await this.walk(path);
    const request = new Writer(); request.u32(fid); request.u64(0); request.u32(MSIZE - 24);
    const reply = await this.rpc(TREAD, RREAD, request);
    const count = new DataView(reply.body.buffer, reply.body.byteOffset).getUint32(0, true);
    const data = reply.body.slice(4, 4 + count);
    return encoding === "utf8" ? new TextDecoder().decode(data) : data;
  }

  async writeFile(path: string, input: string | Uint8Array): Promise<void> {
    await this.connect();
    const fid = await this.walk(path);
    const data = typeof input === "string" ? new TextEncoder().encode(input) : input;
    const request = new Writer(); request.u32(fid); request.u64(0); request.data(data);
    const reply = await this.rpc(TWRITE, RWRITE, request);
    const written = new DataView(reply.body.buffer, reply.body.byteOffset).getUint32(0, true);
    if (written !== data.length) throw new Error(`short 9P write: ${written}/${data.length}`);
  }

  async readJson<T = unknown>(path: string): Promise<T> {
    return JSON.parse(await this.readFile(path, "utf8") as string) as T;
  }
  async writeJson(path: string, value: unknown): Promise<void> {
    await this.writeFile(path, JSON.stringify(value));
  }
  async invoke<T = unknown>(name: string, args: unknown): Promise<T> {
    const path = `/fn/${name}`;
    await this.writeJson(path, args);
    return this.readJson<T>(path);
  }

  private async walk(path: string) {
    const root = this.nextFid++, fid = this.nextFid++;
    const attach = new Writer();
    attach.u32(root); attach.u32(NOFID); attach.string("legion");
    attach.string(this.options.capability ? `cap=${this.options.capability}` : ""); attach.u32(NOFID);
    await this.rpc(TATTACH, RATTACH, attach);
    const parts = path.split("/").filter(Boolean);
    const walk = new Writer(); walk.u32(root); walk.u32(fid); walk.u16(parts.length);
    for (const part of parts) walk.string(part);
    await this.rpc(TWALK, RWALK, walk);
    return fid;
  }

  private rpc(type: number, expected: number, writer: Writer, fixedTag?: number): Promise<Reply> {
    if (!this.socket) throw new Error("9P client is not connected");
    const tag = fixedTag ?? this.nextTag++;
    return new Promise((resolve, reject) => {
      this.pending.set(tag, {
        resolve: (reply) => {
          if (reply.type === RLERROR) {
            const errno = new DataView(reply.body.buffer, reply.body.byteOffset).getUint32(0, true);
            reject(new Error(`9P error ${errno}`));
          } else if (reply.type !== expected) reject(new Error(`unexpected 9P response ${reply.type}, expected ${expected}`));
          else resolve(reply);
        }, reject,
      });
      this.socket!.write(writer.finish(type, tag));
    });
  }

  private onData(chunk: Uint8Array) {
    const combined = new Uint8Array(this.buffer.length + chunk.length);
    combined.set(this.buffer); combined.set(chunk, this.buffer.length); this.buffer = combined;
    while (this.buffer.length >= 7) {
      const length = new DataView(this.buffer.buffer, this.buffer.byteOffset).getUint32(0, true);
      if (this.buffer.length < length) return;
      const frame = this.buffer.slice(0, length); this.buffer = this.buffer.slice(length);
      const tag = new DataView(frame.buffer, frame.byteOffset).getUint16(5, true);
      const pending = this.pending.get(tag);
      if (pending) { this.pending.delete(tag); pending.resolve({ type: frame[4], tag, body: frame.slice(7) }); }
    }
  }

  private fail(error: Error) {
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

export function createLegionFs(options?: LegionFsOptions) { return new LegionFs(options); }
