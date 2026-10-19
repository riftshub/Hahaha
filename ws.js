// Minimal, dependency-free WebSocket server (RFC 6455) on top of Node's http server.
// Supports text frames, fragmentation, ping/pong and close. Enough for game state relay.
import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MESSAGE = 256 * 1024;

export class WSConnection extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = null;
    this.open = true;
    this.alive = true;
    socket.setNoDelay(true);
    socket.on('data', (d) => this.onData(d));
    socket.on('close', () => this.finish());
    socket.on('error', () => this.finish());
  }

  finish() {
    if (!this.open) return;
    this.open = false;
    this.emit('close');
  }

  onData(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    if (this.buf.length > MAX_MESSAGE * 2) return this.close(1009);
    while (this.open) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0;
      const op = b[0] & 0x0f;
      const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) {
        if (b.length < 10) return;
        const hi = b.readUInt32BE(2);
        if (hi !== 0) return this.close(1009);
        len = b.readUInt32BE(6); off = 10;
      }
      if (len > MAX_MESSAGE) return this.close(1009);
      if (!masked) return this.close(1002); // clients must mask
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.handleFrame(fin, op, payload);
    }
  }

  handleFrame(fin, op, payload) {
    if (op === 0x8) { this.close(1000); return; }
    if (op === 0x9) { this.sendFrame(0xA, payload); return; }
    if (op === 0xA) { this.alive = true; return; }
    if (op === 0x1 || op === 0x2) {
      if (fin) this.emit('message', payload.toString('utf8'));
      else this.frag = [payload];
      return;
    }
    if (op === 0x0 && this.frag) {
      this.frag.push(payload);
      if (fin) { const all = Buffer.concat(this.frag); this.frag = null; this.emit('message', all.toString('utf8')); }
    }
  }

  sendFrame(op, data) {
    if (!this.open || this.socket.destroyed) return;
    const len = data.length;
    let head;
    if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
    else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    head[0] = 0x80 | op;
    this.socket.write(Buffer.concat([head, data]));
  }

  send(str) { this.sendFrame(0x1, Buffer.from(str, 'utf8')); }
  ping() { this.alive = false; this.sendFrame(0x9, Buffer.alloc(0)); }

  close(code = 1000) {
    if (!this.open) return;
    const b = Buffer.alloc(2); b.writeUInt16BE(code, 0);
    try { this.sendFrame(0x8, b); } catch (e) { /* ignore */ }
    this.socket.end();
    this.finish();
  }
}

export function attachWebSocket(server, path, onConnection) {
  server.on('upgrade', (req, socket) => {
    const url = (req.url || '').split('?')[0];
    const key = req.headers['sec-websocket-key'];
    if (url !== path || !key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') {
      socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write([
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${accept}`,
      '', '',
    ].join('\r\n'));
    const conn = new WSConnection(socket);
    conn.ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').toString().split(',')[0].trim();
    onConnection(conn, req);
  });
}
