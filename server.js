// Backrooms co-op relay server — zero dependencies, built for Render (free or paid tier).
//
// Model: the room HOST simulates entities (host-authoritative AI); every client simulates its own
// player (client-authoritative movement). The server relays, timestamps, validates, and keeps rooms
// alive across the connection drops that are common on mobile networks and Render's proxy.
//
// Protocol: JSON text frames. See docs/ARCHITECTURE.md for the full message table.
import http from 'node:http';
import crypto from 'node:crypto';
import { attachWebSocket } from './ws.js';

const PORT = Number(process.env.PORT) || 10000;
const MAX_PLAYERS = 8;
const MAX_ROOMS = 400;
const RESUME_GRACE_MS = 25_000;   // a dropped player keeps their slot this long
const HEARTBEAT_MS = 15_000;      // WS ping interval (Render's proxy idles out silent sockets)
const IDLE_KICK_MS = 45_000;      // no traffic at all for this long => dead
const ROOM_EMPTY_TTL_MS = 60_000; // room survives briefly with zero live connections (host phone lock)

const rooms = new Map();     // code -> room
const sessions = new Map();  // token -> player
let nextId = 1;
const now = () => Date.now();

// ---------------------------------------------------------------- helpers
function roomCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c;
  do { c = Array.from({ length: 5 }, () => A[crypto.randomInt(A.length)]).join(''); } while (rooms.has(c));
  return c;
}
const cleanName = (n) => String(n ?? '').replace(/[^\w \-.']/g, '').trim().slice(0, 16) || 'Wanderer';
const num = (v, d = 0) => (Number.isFinite(+v) ? +v : d);
function send(p, msg) { if (p.conn && p.conn.open) p.conn.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); }
function broadcast(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const p of room.players.values()) if (p !== except) send(p, s);
}
function roomState(room, you) {
  return {
    t: 'room', code: room.code, you: you.id, token: you.token, host: room.host,
    seed: room.seed, level: room.level, lseed: room.lseed, cycle: room.cycle, ts: now(),
    players: [...room.players.values()].map((p) => ({ id: p.id, name: p.name, anim: p.anim, live: !!p.conn })),
  };
}
function pickHost(room) {
  // prefer a connected player
  for (const p of room.players.values()) if (p.conn) return p.id;
  return room.players.keys().next().value;
}

function removePlayer(p, reason = 'left') {
  const room = p.room;
  sessions.delete(p.token);
  if (!room) return;
  room.players.delete(p.id);
  p.room = null;
  if (!room.players.size) { rooms.delete(room.code); log(`room ${room.code} closed`); return; }
  broadcast(room, { t: 'leave', id: p.id, reason });
  if (room.host === p.id) { room.host = pickHost(room); broadcast(room, { t: 'host', id: room.host }); }
}

function log(...a) { console.log(new Date().toISOString().slice(11, 19), ...a); }

// ---------------------------------------------------------------- HTTP
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  const url = (req.url || '/').split('?')[0];
  if (url === '/health' || url === '/healthz') {
    let players = 0;
    for (const r of rooms.values()) players += r.players.size;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, players, uptime: Math.round(process.uptime()) }));
    return;
  }
  if (url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Backrooms co-op server is running. Connect the game to wss://<this-host>/ws\n');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

// ---------------------------------------------------------------- WebSocket
attachWebSocket(server, '/ws', (conn) => {
  let p = null; // bound player (after hello)
  conn.lastSeen = now();
  conn.tokens = 90; conn.lastRefill = now();

  conn.on('message', (raw) => {
    conn.lastSeen = now();
    // token bucket: ~60 msgs/s sustained, bursts to 120
    const t = now();
    conn.tokens = Math.min(120, conn.tokens + ((t - conn.lastRefill) / 1000) * 60);
    conn.lastRefill = t;
    if (conn.tokens < 1) return;
    conn.tokens--;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    // clock sync works before hello so clients can measure RTT while waking
    if (m.t === 'ping') { conn.send(JSON.stringify({ t: 'pong', c: m.c, s: now() })); return; }

    if (m.t === 'hello') {
      const old = m.token && sessions.get(m.token);
      if (old && old.room) {
        // resume: rebind the socket to the existing player slot
        if (old.conn && old.conn !== conn) old.conn.close(4000);
        p = old; p.conn = conn; p.droppedAt = 0;
        send(p, roomState(p.room, p));
        broadcast(p.room, { t: 'back', id: p.id }, p);
        if (p.room.host === p.id || !p.room.players.get(p.room.host)?.conn) {
          // host lost? make sure someone live is hosting
          const h = pickHost(p.room);
          if (h !== p.room.host) { p.room.host = h; broadcast(p.room, { t: 'host', id: h }); }
        }
        log(`${p.name} resumed in ${p.room.code}`);
      } else {
        p = { id: 'p' + nextId++, token: crypto.randomBytes(16).toString('hex'), name: 'Wanderer', conn, room: null, anim: 0, droppedAt: 0 };
        sessions.set(p.token, p);
        send(p, { t: 'welcome', you: p.id, token: p.token, s: now() });
      }
      return;
    }
    if (!p) { conn.send(JSON.stringify({ t: 'err', code: 'nohello', msg: 'Send hello first.' })); return; }

    switch (m.t) {
      case 'create': {
        if (p.room) removePlayer(p, 'switched');
        sessions.set(p.token, p);
        if (rooms.size >= MAX_ROOMS) return send(p, { t: 'err', code: 'full', msg: 'Server is full, try again later.' });
        p.name = cleanName(m.name);
        const room = {
          code: roomCode(), host: p.id, players: new Map(),
          seed: (num(m.seed) >>> 0) || crypto.randomInt(2 ** 31),
          level: Math.max(0, Math.min(64, num(m.level) | 0)), lseed: 0, cycle: 0, emptySince: 0,
        };
        room.players.set(p.id, p);
        p.room = room;
        rooms.set(room.code, room);
        send(p, roomState(room, p));
        log(`room ${room.code} created by ${p.name}`);
        return;
      }
      case 'join': {
        const room = rooms.get(String(m.code || '').toUpperCase().trim());
        if (!room) return send(p, { t: 'err', code: 'noroom', msg: 'No room with that code.' });
        if (room.players.size >= MAX_PLAYERS && !room.players.has(p.id)) return send(p, { t: 'err', code: 'roomfull', msg: 'That room is full.' });
        if (p.room && p.room !== room) removePlayer(p, 'switched');
        sessions.set(p.token, p);
        p.name = cleanName(m.name);
        room.players.set(p.id, p);
        p.room = room;
        send(p, roomState(room, p));
        broadcast(room, { t: 'join', id: p.id, name: p.name }, p);
        return;
      }
      case 'leave': removePlayer(p, 'left'); return;
      default: break;
    }

    const room = p.room;
    if (!room) return;
    const isHost = room.host === p.id;

    if (m.t === 'st') {
      // player transform + animation state. Validated & timestamped for client-side interpolation.
      const pos = Array.isArray(m.p) ? m.p.slice(0, 3).map((v) => num(v)) : null;
      if (!pos) return;
      p.anim = num(m.a) | 0;
      broadcast(room, { t: 'st', from: p.id, ts: now(), p: pos, r: Array.isArray(m.r) ? m.r.slice(0, 2).map((v) => num(v)) : [0, 0], a: p.anim, f: num(m.f) | 0, v: Array.isArray(m.v) ? m.v.slice(0, 3).map((v) => num(v)) : undefined }, p);
      return;
    }
    if (m.t === 'ent') {
      if (!isHost || !Array.isArray(m.s)) return;
      broadcast(room, { t: 'ent', ts: now(), s: m.s, pw: num(m.pw, 1) }, p);
      return;
    }
    if (m.t === 'ev') {
      const k = String(m.k || '');
      // host-only authoritative events
      if (['level', 'kill', 'loot', 'spawn'].includes(k) && !isHost) return;
      if (k === 'level') { room.level = num(m.level) | 0; room.lseed = num(m.lseed) >>> 0; room.cycle = num(m.cycle) | 0; }
      const out = { ...m, from: p.id, ts: now() };
      broadcast(room, out, k === 'level' ? p : null); // echo non-level events so the sender sees host-ordered results too
    }
  });

  conn.on('close', () => {
    if (!p || p.conn !== conn) return;
    p.conn = null;
    p.droppedAt = now();
    if (p.room) {
      broadcast(p.room, { t: 'lag', id: p.id });
      // hand host to a live player immediately so entities keep moving
      if (p.room.host === p.id) {
        const h = pickHost(p.room);
        if (h !== p.id) { p.room.host = h; broadcast(p.room, { t: 'host', id: h }); }
      }
    } else sessions.delete(p.token);
  });
});

// ---------------------------------------------------------------- maintenance loops
setInterval(() => {
  const t = now();
  for (const room of rooms.values()) {
    let live = 0;
    for (const p of [...room.players.values()]) {
      if (p.conn) {
        live++;
        if (!p.conn.alive || t - p.conn.lastSeen > IDLE_KICK_MS) { p.conn.close(4001); continue; }
        p.conn.ping();
      } else if (p.droppedAt && t - p.droppedAt > RESUME_GRACE_MS) {
        log(`${p.name} timed out of ${room.code}`);
        removePlayer(p, 'timeout');
      }
    }
    if (!live) { room.emptySince ||= t; if (t - room.emptySince > ROOM_EMPTY_TTL_MS) { for (const p of room.players.values()) sessions.delete(p.token); rooms.delete(room.code); } }
    else room.emptySince = 0;
  }
}, HEARTBEAT_MS / 3);

// Render sends SIGTERM on every deploy / spin-down. Tell clients so they show "reconnecting" instead of hanging.
function shutdown() {
  log('shutting down');
  for (const room of rooms.values()) broadcast(room, { t: 'bye', msg: 'Server restarting — reconnecting…' });
  setTimeout(() => process.exit(0), 300);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.keepAliveTimeout = 65_000; // longer than Render's load balancer idle timeout
server.headersTimeout = 66_000;
server.listen(PORT, () => log(`Backrooms co-op server listening on :${PORT}`));
