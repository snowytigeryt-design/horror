'use strict';
/*
  The Hollow Ward: co-op server.
  - Serves ./public over HTTP
  - /ws: WebSocket game relay (rooms of up to 4 players)
  - NeDB (two files in DATA_DIR): players.db (one record per player uuid) and games.db (one record per run)
  The server does not simulate the monster. The room host's browser does, and the server relays it.
*/
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const Datastore = require('nedb');

process.on('uncaughtException', (e) => console.error('[uncaught]', (e && e.stack) || e));
process.on('unhandledRejection', (e) => console.error('[unhandled]', (e && e.stack) || e));

const PORT = parseInt(process.env.PORT, 10) || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_PLAYERS = 4;
const RECONNECT_GRACE_MS = 60 * 1000; /* how long a dropped player keeps their seat in a room */
const NFUSE = 5;
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

fs.mkdirSync(DATA_DIR, { recursive: true });
const onload = (name) => (err) => { if (err) console.error('[db] could not load ' + name, err); };
const players = new Datastore({ filename: path.join(DATA_DIR, 'players.db'), autoload: true, onload: onload('players.db') });
const games = new Datastore({ filename: path.join(DATA_DIR, 'games.db'), autoload: true, onload: onload('games.db') });
try {
  players.ensureIndex({ fieldName: 'uuid', unique: true });
  players.persistence.setAutocompactionInterval(10 * 60 * 1000);
  games.persistence.setAutocompactionInterval(10 * 60 * 1000);
} catch (e) { console.error('[db] setup', e); }

const DB_TIMEOUT = 4000;
const p = (fn) => new Promise((res, rej) => {
  const t = setTimeout(() => rej(new Error('db timeout')), DB_TIMEOUT);
  try { fn((e, r) => { clearTimeout(t); if (e) rej(e); else res(r); }); }
  catch (e) { clearTimeout(t); rej(e); }
});
const dbFindOne = (db, q) => p((cb) => db.findOne(q, cb));
const dbInsert = (db, d) => p((cb) => db.insert(d, cb));
const dbUpdate = (db, q, u) => p((cb) => db.update(q, u, {}, cb));
const dbTop = () => p((cb) => players.find({ escapes: { $gt: 0 } }).sort({ escapes: -1, bestMs: 1 }).limit(10).exec(cb));

/* ---------- helpers ---------- */
const now = () => Date.now();
const rid = () => crypto.randomBytes(4).toString('hex');
const cleanName = (s) => String(s || '').replace(/[^\w \-]/g, '').trim().slice(0, 16) || 'Survivor' + Math.floor(Math.random() * 900 + 100);
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v) ? Math.max(lo, Math.min(hi, v)) : 0);
const send = (c, o) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(o)); };

/* ---------- rooms ---------- */
const rooms = new Map();

function newCode() {
  for (let i = 0; i < 200; i++) {
    let s = '';
    for (let k = 0; k < 4; k++) s += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}
function makeRoom(isPublic) {
  const code = newCode();
  if (!code) return null;
  const r = { code, isPublic: !!isPublic, state: 'lobby', host: null, seed: 0, members: new Map(), fuses: new Set(), startedAt: 0, gameId: null, lastMon: now(), created: now() };
  rooms.set(code, r);
  return r;
}
function roomView(r, c) {
  return {
    t: 'room', code: r.code, state: r.state, host: r.host, you: c.pid,
    seed: r.state === 'playing' ? r.seed : 0,
    fuses: Array.from(r.fuses),
    members: Array.from(r.members.values()).map((m) => ({ id: m.pid, name: m.name, alive: m.alive, escaped: m.escaped }))
  };
}
function pushRoom(r) { for (const c of r.members.values()) send(c, roomView(r, c)); }
function relay(r, from, o) { for (const c of r.members.values()) if (c !== from) send(c, o); }

function joinRoom(c, r) {
  if (c.room) leaveRoom(c);
  c.room = r; c.pid = rid(); c.alive = true; c.escaped = false; c.joinedAt = now(); c.hidden = -1;
  r.members.set(c.pid, c);
  if (!r.host) r.host = c.pid;
  pushRoom(r);
}
function leaveRoom(c) {
  const r = c.room;
  if (!r) return;
  r.members.delete(c.pid);
  relay(r, c, { t: 'left', id: c.pid });
  c.room = null;
  if (r.members.size === 0) { endGame(r, 'empty'); rooms.delete(r.code); return; }
  if (r.host === c.pid) { r.host = r.members.keys().next().value; r.lastMon = now(); }
  checkAllDone(r);
  pushRoom(r);
}
function checkAllDone(r) {
  if (r.state !== 'playing') return;
  const m = Array.from(r.members.values());
  if (m.length && m.every((x) => x.escaped)) {
    endGame(r, 'escaped');
    for (const c of r.members.values()) send(c, { t: 'end', result: 'escaped' });
  }
}
function endGame(r, result) {
  if (r.gameId && r.state !== 'ended') {
    dbUpdate(games, { _id: r.gameId }, { $set: { endedAt: now(), result } }).catch(logErr);
  }
  if (r.state === 'playing') r.state = 'ended';
}
const logErr = (e) => console.error('[db]', e && e.message ? e.message : e);

/* ---------- player records ---------- */
async function loadPlayer(uuid, name) {
  let rec = await dbFindOne(players, { uuid });
  if (!rec) {
    rec = { uuid, name, createdAt: now(), lastSeen: now(), games: 0, escapes: 0, deaths: 0, fuses: 0, bestMs: null };
    try { rec = await dbInsert(players, rec); }
    catch (e) { rec = await dbFindOne(players, { uuid }); if (!rec) throw e; }
  } else {
    await dbUpdate(players, { uuid }, { $set: { name, lastSeen: now() } });
    rec.name = name;
  }
  return rec;
}
const bump = (uuid, inc) => dbUpdate(players, { uuid }, { $inc: inc }).catch(logErr);

/* ---------- message handling ---------- */
const byUuid = new Map();

async function onMessage(c, m) {
  if (!m || typeof m.t !== 'string') return;
  if (m.t === 'hello') {
    if (c.uuid) return;
    if (typeof m.uuid !== 'string' || !UUID_RE.test(m.uuid)) { send(c, { t: 'error', msg: 'Bad player id.' }); return; }
    const uuid = m.uuid.toLowerCase();
    const old = byUuid.get(uuid);
    let resumed = false;
    if (old && old !== c) {
      if (old.room) {
        /* Same player is already in a room (dropped connection, or a half-open old socket):
           hand the existing session to this new socket instead of kicking them out. */
        clearTimeout(old.grace); old.grace = null; old.dead = false;
        const oldWs = old.ws;
        old.ws = c.ws; c.ws.client = old;
        if (oldWs && oldWs !== c.ws && oldWs.readyState === 1) {
          try { oldWs.send(JSON.stringify({ t: 'error', msg: 'Signed in from another tab.', kicked: true })); oldWs.close(); } catch (e) { /* ignore */ }
        }
        c = old; resumed = true;
      } else {
        send(old, { t: 'error', msg: 'Signed in from another tab.', kicked: true }); old.ws.close();
      }
    }
    c.uuid = uuid; c.name = cleanName(m.name); byUuid.set(uuid, c);
    const inRoom = resumed && !!c.room;
    try {
      const rec = await Promise.race([loadPlayer(uuid, c.name), new Promise((_, rej) => setTimeout(() => rej(new Error('player record is slow')), 1500))]);
      send(c, { t: 'welcome', name: c.name, resumed: inRoom, stats: { games: rec.games, escapes: rec.escapes, deaths: rec.deaths, fuses: rec.fuses, bestMs: rec.bestMs } });
    } catch (e) { logErr(e); send(c, { t: 'welcome', name: c.name, resumed: inRoom, stats: { games: 0, escapes: 0, deaths: 0, fuses: 0, bestMs: null }, noRecord: true }); }
    if (inRoom && c.room) pushRoom(c.room);
    return;
  }
  if (!c.uuid) return;
  const r = c.room;
  switch (m.t) {
    case 'create': { const room = makeRoom(false); if (!room) return send(c, { t: 'error', msg: 'Server is full.' }); joinRoom(c, room); break; }
    case 'quick': {
      let room = null;
      for (const x of rooms.values()) if (x.isPublic && x.state === 'lobby' && x.members.size < MAX_PLAYERS) { room = x; break; }
      if (!room) room = makeRoom(true);
      if (!room) return send(c, { t: 'error', msg: 'Server is full.' });
      joinRoom(c, room); break;
    }
    case 'join': {
      const code = String(m.code || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4);
      const room = rooms.get(code);
      if (!room) return send(c, { t: 'error', msg: 'No room with that code.' });
      if (room.state === 'ended') return send(c, { t: 'error', msg: 'That run has ended.' });
      if (room.members.size >= MAX_PLAYERS) return send(c, { t: 'error', msg: 'That room is full.' });
      joinRoom(c, room);
      if (room.state === 'playing') bump(c.uuid, { games: 1 });
      break;
    }
    case 'leave': if (r) leaveRoom(c); break;
    case 'start': {
      if (!r || r.state !== 'lobby' || r.host !== c.pid) return;
      r.state = 'playing'; r.seed = crypto.randomInt(1, 2 ** 31); r.startedAt = now(); r.lastMon = now();
      for (const x of r.members.values()) { x.alive = true; x.escaped = false; x.joinedAt = now(); bump(x.uuid, { games: 1 }); }
      dbInsert(games, { code: r.code, seed: r.seed, startedAt: r.startedAt, endedAt: null, result: null, players: Array.from(r.members.values()).map((x) => x.name) })
        .then((g) => { r.gameId = g._id; }).catch(logErr);
      for (const x of r.members.values()) send(x, { t: 'start', seed: r.seed, you: x.pid });
      pushRoom(r);
      break;
    }
    case 'p': {
      if (!r || r.state !== 'playing') return;
      c.hidden = Math.floor(num(m.h, -1, 40));
      relay(r, c, { t: 'p', id: c.pid, x: num(m.x, -50, 200), z: num(m.z, -50, 200), y: num(m.y, -100, 100), m: m.m ? 1 : 0, s: m.s ? 1 : 0, l: m.l ? 1 : 0, h: c.hidden });
      break;
    }
    case 'mon': {
      if (!r || r.state !== 'playing' || r.host !== c.pid) return;
      r.lastMon = now();
      relay(r, c, { t: 'mon', x: num(m.x, -50, 200), z: num(m.z, -50, 200), f: num(m.f, -100, 100), s: String(m.s || 'patrol').slice(0, 12), m: m.m ? 1 : 0, a: m.a ? 1 : 0, g: typeof m.g === 'string' ? m.g.slice(0, 12) : '', w: m.w ? 1 : 0 });
      break;
    }
    case 'fuse': {
      if (!r || r.state !== 'playing') return;
      const i = m.i;
      if (!Number.isInteger(i) || i < 0 || i >= NFUSE || r.fuses.has(i)) return;
      r.fuses.add(i);
      bump(c.uuid, { fuses: 1 });
      for (const x of r.members.values()) send(x, { t: 'fuse', i, by: c.pid, n: r.fuses.size });
      break;
    }
    case 'kill': {
      if (!r || r.host !== c.pid) return;
      const t = r.members.get(String(m.to || ''));
      if (t && t !== c) send(t, { t: 'kill', k: String(m.k || 'catch').slice(0, 16) });
      break;
    }
    case 'died': {
      if (!r || r.state !== 'playing' || !c.alive) return;
      c.alive = false;
      bump(c.uuid, { deaths: 1 });
      for (const x of r.members.values()) send(x, { t: 'died', id: c.pid, k: String(m.k || 'catch').slice(0, 16) });
      break;
    }
    case 'respawn': {
      if (!r || r.state !== 'playing' || c.alive || c.escaped) return;
      c.alive = true;
      relay(r, c, { t: 'respawn', id: c.pid });
      break;
    }
    case 'escaped': {
      if (!r || r.state !== 'playing' || c.escaped || r.fuses.size < NFUSE) return;
      c.escaped = true;
      const ms = now() - (c.joinedAt || r.startedAt);
      for (const x of r.members.values()) send(x, { t: 'escaped', id: c.pid, ms });
      checkAllDone(r);
      (async () => {
        const rec = await dbFindOne(players, { uuid: c.uuid });
        const set = {};
        if (rec && (rec.bestMs == null || ms < rec.bestMs)) set.bestMs = ms;
        await dbUpdate(players, { uuid: c.uuid }, Object.keys(set).length ? { $inc: { escapes: 1 }, $set: set } : { $inc: { escapes: 1 } });
      })().catch(logErr);
      break;
    }
    case 'say': {
      if (!r) return;
      const k = Math.floor(num(m.k, 0, 2));
      relay(r, c, { t: 'say', id: c.pid, k });
      break;
    }
    case 'ping': send(c, { t: 'pong' }); break;
  }
}

/* ---------- http ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8' };
const server = http.createServer(async (req, res) => {
  let pathname;
  try { pathname = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (pathname === '/favicon.ico') { res.writeHead(204); return res.end(); }
  if (pathname === '/vendor/three.min.js') {
    return fs.readFile(path.join(__dirname, 'node_modules', 'three', 'build', 'three.min.js'), (err, buf) => {
      if (err) { res.writeHead(404); return res.end(); }
      res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'public, max-age=86400' });
      res.end(buf);
    });
  }
  if (pathname === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (pathname === '/api/leaderboard') {
    try {
      const top = await dbTop();
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify(top.map((x) => ({ name: x.name, escapes: x.escapes, bestMs: x.bestMs }))));
    } catch (e) { logErr(e); res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end('[]'); }
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  if (pathname === '/') pathname = '/index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, pathname));
  if (file !== PUBLIC_DIR && !file.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(req.method === 'HEAD' ? undefined : buf);
  });
});

/* ---------- websocket ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4096 });
wss.on('connection', (ws) => {
  const c = { ws, uuid: null, name: '', pid: null, room: null, alive: true, escaped: false, joinedAt: 0, hidden: -1, tokens: 80, last: now(), isAlive: true, dead: false, grace: null };
  ws.client = c; /* may be swapped to an older session when a player reconnects */
  ws.missed = 0;
  /* FIX: the pong must be recorded on the socket, because the heartbeat below checks the socket.
     Before, it was recorded on the client object, so every connection was terminated after ~50s. */
  ws.on('pong', () => { ws.missed = 0; });
  ws.on('message', (data) => {
    ws.missed = 0; /* any traffic proves the connection is alive */
    const cl = ws.client;
    const t = now();
    cl.tokens = Math.min(80, cl.tokens + ((t - cl.last) / 1000) * 60); cl.last = t;
    if (cl.tokens < 1) return;
    cl.tokens -= 1;
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    onMessage(cl, m).catch(logErr);
  });
  ws.on('close', () => {
    const cl = ws.client;
    if (cl.ws !== ws) return; /* this socket was already replaced by a newer one for the same player */
    if (cl.uuid && cl.room) {
      /* keep the player's seat for a short while so a dropped connection can resume instead of being kicked */
      cl.dead = true;
      clearTimeout(cl.grace);
      cl.grace = setTimeout(() => {
        cl.grace = null;
        if (!cl.dead) return;
        if (cl.uuid && byUuid.get(cl.uuid) === cl) byUuid.delete(cl.uuid);
        if (cl.room) leaveRoom(cl);
      }, RECONNECT_GRACE_MS);
      return;
    }
    if (cl.uuid && byUuid.get(cl.uuid) === cl) byUuid.delete(cl.uuid);
    if (cl.room) leaveRoom(cl);
  });
  ws.on('error', () => {});
});

/* keep connections alive through Render's proxy; drop dead ones (allows 2 missed pings before giving up) */
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.missed >= 2) { ws.terminate(); continue; }
    ws.missed = (ws.missed || 0) + 1; try { ws.ping(); } catch (e) { /* ignore */ }
  }
}, 20000);
/* host watchdog: if the host's tab stops sending monster updates (background tab), hand the role to someone else */
setInterval(() => {
  const t = now();
  for (const r of rooms.values()) {
    if (r.state === 'playing' && r.members.size > 1 && t - r.lastMon > 4000) {
      const ids = Array.from(r.members.keys()), i = ids.indexOf(r.host);
      let next = r.host;
      for (let k = 1; k <= ids.length; k++) { const cand = ids[(i + k) % ids.length]; if (!r.members.get(cand).dead) { next = cand; break; } }
      r.host = next; r.lastMon = t; pushRoom(r);
    }
    if (r.state !== 'playing' && t - r.created > 6 * 3600 * 1000 && r.members.size === 0) rooms.delete(r.code);
  }
}, 1000);

server.listen(PORT, () => console.log('The Hollow Ward listening on ' + PORT + ', data in ' + DATA_DIR));
module.exports = { server, wss, rooms };
