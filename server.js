// ============================================================
// เซิร์ฟเวอร์ออนไลน์ "ศึกชิงแผ่นดิน"
// - บัญชีผู้เล่น (สมัคร/ล็อกอิน รหัสผ่านเข้ารหัสแบบ scrypt) + สถิติแพ้ชนะ
// - ล็อบบี้: สร้างห้อง (มีรหัสห้อง 5 ตัว) / เข้าห้อง / ที่นั่ง 2-4 ที่ / บอทเติมที่ว่าง
// - ระหว่างเกม: เซิร์ฟเวอร์ไม่รู้กติกา ทำหน้าที่ "เรียงลำดับคำสั่ง" แล้วส่งให้ทุกเครื่อง
//   ทุกเครื่องรันเกมชุดเดียวกันด้วย seed เดียวกัน จึงได้ผลตรงกัน (lockstep)
// - คนหลุด: รอ 15 วิ แล้วให้บอทเล่นแทน กลับเข้ามาได้ (ส่งประวัติคำสั่งให้เล่นตามทัน)
// - เจ้าของห้องหลุด: ผู้เล่นคนถัดไปรับหน้าที่คุมบอทแทนอัตโนมัติ
// รัน: npm install แล้ว node server.js   (พอร์ตจากตัวแปร PORT ค่าเริ่มต้น 8080)
// ============================================================
'use strict';
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { makeStore } = require('./store');

const PROTOCOL = 1;
const PORT = Number(process.env.PORT || 8080);
const RECONNECT_GRACE_MS = Number(process.env.RECONNECT_GRACE_MS || 15000);
const EMPTY_GAME_TTL_MS = Number(process.env.EMPTY_GAME_TTL_MS || 10 * 60 * 1000);
const SESSION_DAYS = 30;
const MAX_ROOMS = 200;
const MAX_LOG = 20000;

const store = makeStore();

// ---------------- บัญชี ----------------
const NAME_RE = /^[A-Za-z0-9_฀-๿]{3,16}$/;

function hashPassword(pass, salt) {
  return new Promise((res, rej) => crypto.scrypt(pass, salt, 64, { N: 16384 }, (e, k) => e ? rej(e) : res(k.toString('hex'))));
}
function userKey(name) { return name.trim().toLowerCase(); }
function newToken() { return crypto.randomBytes(24).toString('hex'); }

// จำกัดการลองรหัสผิดต่อ IP (กันเดารหัส)
const attempts = new Map();
function tooManyAttempts(ip) {
  const now = Date.now();
  const a = attempts.get(ip) || { n: 0, t: now };
  if (now - a.t > 10 * 60 * 1000) { a.n = 0; a.t = now; }
  attempts.set(ip, a);
  return a.n >= 20;
}
function noteFailedAttempt(ip) { const a = attempts.get(ip); if (a) a.n += 1; }

// ---------------- ห้อง ----------------
const rooms = new Map();          // code -> room
const clients = new Set();        // ws ทั้งหมดที่ล็อกอินแล้ว

function roomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (;;) {
    let s = '';
    for (let i = 0; i < 5; i++) s += chars[crypto.randomInt(chars.length)];
    if (!rooms.has(s)) return s;
  }
}

function seatView(s) {
  return { kind: s.kind, name: s.name || '', connected: !!s.ws, ready: !!s.ready, bot_control: !!s.botControl };
}
function roomView(r) {
  return {
    code: r.code, name: r.name, max: r.max, status: r.status, host_seat: r.hostSeat,
    seats: r.seats.map(seatView),
  };
}
function roomSummary(r) {
  return {
    code: r.code, name: r.name, max: r.max, status: r.status,
    humans: r.seats.filter(s => s.kind === 'human').length,
    open: r.seats.filter(s => s.kind === 'open').length,
    host: (r.seats[r.hostSeat] || {}).name || '',
  };
}

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}
function broadcast(r, msg) {
  for (const s of r.seats) if (s.ws) send(s.ws, msg);
}
function broadcastRoom(r) {
  for (let i = 0; i < r.seats.length; i++) {
    const s = r.seats[i];
    if (s.ws) send(s.ws, { t: 'room', room: roomView(r), your_seat: i });
  }
}
function lobbyBroadcast() {
  const list = [...rooms.values()].filter(r => r.status === 'waiting').map(roomSummary);
  for (const ws of clients) if (ws.lobby) send(ws, { t: 'rooms', rooms: list });
}

// เจ้าของห้อง/ผู้คุมบอท = ผู้เล่นคนจริงที่ต่ออยู่ ที่นั่งเลขน้อยสุด
function pickHost(r) {
  const old = r.hostSeat;
  let h = -1;
  for (let i = 0; i < r.seats.length; i++) {
    if (r.seats[i].kind === 'human' && r.seats[i].ws) { h = i; break; }
  }
  if (h === -1 && r.status === 'waiting') {
    // ห้องรอที่ไม่มีคนเหลือ = ลบทิ้ง
    rooms.delete(r.code);
    lobbyBroadcast();
    return;
  }
  r.hostSeat = h;
  if (r.status === 'playing' && h !== old && h !== -1) broadcast(r, { t: 'host', seat: h });
}

function findSeatOfUser(r, key) {
  return r.seats.findIndex(s => s.kind === 'human' && s.key === key);
}

function leaveRoom(ws, voluntary) {
  const r = ws.room;
  if (!r) return;
  const i = ws.seat;
  ws.room = null; ws.seat = -1;
  const s = r.seats[i];
  if (!s || s.ws !== ws) return;
  s.ws = null;
  if (r.status === 'waiting') {
    // ออกจากห้องรอ = คืนที่นั่ง
    r.seats[i] = { kind: 'open' };
    pickHost(r);
    if (rooms.has(r.code)) broadcastRoom(r);
    lobbyBroadcast();
    return;
  }
  // กำลังเล่น: ให้บอทรับช่วง (ทันทีถ้าออกเอง, รอ 15 วิถ้าหลุด)
  const takeOver = () => {
    if (s.ws || s.botControl || r.status !== 'playing') return;
    s.botControl = true;
    broadcast(r, { t: 'seat_control', seat: i, bot: true, name: s.name });
  };
  if (voluntary) { s.left = true; takeOver(); }
  else setTimeout(takeOver, RECONNECT_GRACE_MS);
  broadcast(r, { t: 'player_conn', seat: i, connected: false, name: s.name });
  pickHost(r);
  checkEmptyGame(r);
}

function checkEmptyGame(r) {
  if (r.status !== 'playing') return;
  const anyone = r.seats.some(s => s.kind === 'human' && s.ws);
  if (!anyone) {
    clearTimeout(r.emptyTimer);
    r.emptyTimer = setTimeout(() => {
      if (!r.seats.some(s => s.kind === 'human' && s.ws)) rooms.delete(r.code);
    }, EMPTY_GAME_TTL_MS);
  }
}

function gameStartMsg(r, seat) {
  return {
    t: 'game_start', seed: r.seed, n: r.max, your_seat: seat, host_seat: r.hostSeat,
    seats: r.seats.map(s => ({ kind: s.kind, name: s.name || '', bot_control: !!s.botControl })),
    log: r.log, code: r.code,
  };
}

function joinSeat(ws, r, i) {
  r.seats[i].ws = ws;
  ws.room = r; ws.seat = i; ws.lobby = false;
  if (r.status === 'playing') {
    const s = r.seats[i];
    clearTimeout(r.emptyTimer);
    const wasBot = s.botControl;
    s.botControl = false; s.left = false;
    pickHost(r);
    send(ws, gameStartMsg(r, i));
    broadcast(r, { t: 'player_conn', seat: i, connected: true, name: s.name });
    if (wasBot) broadcast(r, { t: 'seat_control', seat: i, bot: false, name: s.name });
  } else {
    broadcastRoom(r);
    lobbyBroadcast();
  }
}

// ---------------- จัดการข้อความ ----------------
async function onAuth(ws, user, token) {
  // ล็อกอินซ้ำจากที่อื่น: ตัดการเชื่อมต่อเก่า
  for (const other of clients) {
    if (other !== ws && other.user && other.user.key === user.key) {
      send(other, { t: 'kicked', reason: 'เข้าสู่ระบบจากที่อื่น' });
      try { other.close(); } catch (e) {}
    }
  }
  ws.user = { key: user.key, name: user.display };
  clients.add(ws);
  send(ws, { t: 'auth_ok', token, name: user.display, games: user.games, wins: user.wins });
  // ถ้ายังมีเกมที่เล่นค้างอยู่ พาเข้าเกมต่อเลย
  for (const r of rooms.values()) {
    if (r.status !== 'playing') continue;
    const i = findSeatOfUser(r, user.key);
    if (i !== -1 && !r.seats[i].left) {
      joinSeat(ws, r, i);
      return;
    }
  }
  ws.lobby = true;
}

async function handle(ws, msg) {
  const t = msg.t;
  if (t === 'ping') return send(ws, { t: 'pong' });
  if (t === 'hello') return send(ws, { t: 'hello', v: PROTOCOL });

  if (t === 'register' || t === 'login') {
    const name = String(msg.user || '').trim();
    const pass = String(msg.pass || '');
    if (tooManyAttempts(ws.ip)) return send(ws, { t: 'error', code: 'rate', msg: 'ลองหลายครั้งเกินไป รอสักครู่แล้วลองใหม่' });
    if (!NAME_RE.test(name)) return send(ws, { t: 'error', code: 'name', msg: 'ชื่อต้องยาว 3-16 ตัว ใช้ได้เฉพาะภาษาไทย อังกฤษ ตัวเลข และ _' });
    if (pass.length < 6 || pass.length > 64) return send(ws, { t: 'error', code: 'pass', msg: 'รหัสผ่านต้องยาว 6-64 ตัว' });
    const key = userKey(name);
    if (t === 'register') {
      const salt = crypto.randomBytes(16).toString('hex');
      const hash = await hashPassword(pass, salt);
      const ok = await store.createUser({ key, display: name, salt, hash });
      if (!ok) return send(ws, { t: 'error', code: 'taken', msg: 'ชื่อนี้มีคนใช้แล้ว' });
    }
    const u = await store.getUser(key);
    let ok = false;
    if (u) {
      const h = await hashPassword(pass, u.salt);
      ok = crypto.timingSafeEqual(Buffer.from(h, 'hex'), Buffer.from(u.hash, 'hex'));
    }
    if (!ok) {
      noteFailedAttempt(ws.ip);
      return send(ws, { t: 'error', code: 'login', msg: 'ชื่อหรือรหัสผ่านไม่ถูกต้อง' });
    }
    const token = newToken();
    await store.saveSession(token, key, Date.now() + SESSION_DAYS * 86400000);
    return onAuth(ws, u, token);
  }

  if (t === 'resume') {
    const s = await store.getSession(String(msg.token || ''));
    const u = s && await store.getUser(s.key);
    if (!u) return send(ws, { t: 'error', code: 'session', msg: 'กรุณาเข้าสู่ระบบใหม่' });
    return onAuth(ws, u, msg.token);
  }

  if (!ws.user) return send(ws, { t: 'error', code: 'auth', msg: 'ยังไม่ได้เข้าสู่ระบบ' });

  switch (t) {
    case 'logout': {
      leaveRoom(ws, true);
      await store.deleteSession(String(msg.token || ''));
      clients.delete(ws); ws.user = null;
      return send(ws, { t: 'logged_out' });
    }
    case 'me': {
      const u = await store.getUser(ws.user.key);
      return send(ws, { t: 'me', name: u.display, games: u.games, wins: u.wins });
    }
    case 'top': {
      return send(ws, { t: 'top', players: await store.topPlayers(10) });
    }
    case 'list_rooms': {
      ws.lobby = !ws.room;
      return send(ws, { t: 'rooms', rooms: [...rooms.values()].filter(r => r.status === 'waiting').map(roomSummary) });
    }
    case 'create_room': {
      if (ws.room) leaveRoom(ws, true);
      if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'error', code: 'full', msg: 'เซิร์ฟเวอร์เต็ม ลองใหม่ภายหลัง' });
      const max = Math.max(2, Math.min(4, Number(msg.max) || 2));
      let name = String(msg.name || '').trim().slice(0, 24);
      if (!name) name = 'ห้องของ ' + ws.user.name;
      const r = {
        code: roomCode(), name, max, status: 'waiting', hostSeat: 0, log: [], seed: 0,
        seats: Array.from({ length: max }, () => ({ kind: 'open' })), hashes: new Map(),
        reported: false,
      };
      r.seats[0] = { kind: 'human', key: ws.user.key, name: ws.user.name, ready: true };
      rooms.set(r.code, r);
      joinSeat(ws, r, 0);
      return;
    }
    case 'join_room': {
      const code = String(msg.code || '').trim().toUpperCase();
      const r = rooms.get(code);
      if (!r) return send(ws, { t: 'error', code: 'no_room', msg: 'ไม่พบห้องรหัส ' + code });
      const mine = findSeatOfUser(r, ws.user.key);
      if (r.status === 'playing') {
        if (mine === -1) return send(ws, { t: 'error', code: 'started', msg: 'ห้องนี้เริ่มเกมไปแล้ว' });
        if (ws.room && ws.room !== r) leaveRoom(ws, true);
        r.seats[mine].left = false;
        return joinSeat(ws, r, mine);
      }
      if (ws.room === r) return broadcastRoom(r);
      if (ws.room) leaveRoom(ws, true);
      if (mine !== -1) return joinSeat(ws, r, mine);
      const i = r.seats.findIndex(s => s.kind === 'open');
      if (i === -1) return send(ws, { t: 'error', code: 'room_full', msg: 'ห้องเต็มแล้ว' });
      r.seats[i] = { kind: 'human', key: ws.user.key, name: ws.user.name, ready: false };
      return joinSeat(ws, r, i);
    }
    case 'leave_room': {
      leaveRoom(ws, true);
      ws.lobby = true;
      return send(ws, { t: 'left_room' });
    }
    case 'set_seat': {
      const r = ws.room;
      if (!r || r.status !== 'waiting' || ws.seat !== r.hostSeat) return;
      const i = Number(msg.seat);
      if (!(i >= 0 && i < r.max) || i === r.hostSeat) return;
      const kind = msg.kind === 'bot' ? 'bot' : 'open';
      const s = r.seats[i];
      if (s.kind === 'human' && s.ws) {
        // เตะผู้เล่นออก
        const kws = s.ws;
        kws.room = null; kws.seat = -1; kws.lobby = true;
        send(kws, { t: 'kicked_room', msg: 'เจ้าของห้องให้คุณออกจากห้อง' });
      }
      r.seats[i] = kind === 'bot' ? { kind: 'bot', name: 'บอท ' + (i + 1) } : { kind: 'open' };
      broadcastRoom(r); lobbyBroadcast();
      return;
    }
    case 'set_max': {
      const r = ws.room;
      if (!r || r.status !== 'waiting' || ws.seat !== r.hostSeat) return;
      const max = Math.max(2, Math.min(4, Number(msg.max) || 2));
      const humansBeyond = r.seats.slice(max).some(s => s.kind === 'human');
      if (humansBeyond) return send(ws, { t: 'error', code: 'seat_taken', msg: 'มีผู้เล่นนั่งที่นั่งท้าย ๆ อยู่ ลดจำนวนไม่ได้' });
      while (r.seats.length < max) r.seats.push({ kind: 'open' });
      r.seats.length = max; r.max = max;
      broadcastRoom(r); lobbyBroadcast();
      return;
    }
    case 'ready': {
      const r = ws.room;
      if (!r || r.status !== 'waiting') return;
      r.seats[ws.seat].ready = !!msg.on;
      return broadcastRoom(r);
    }
    case 'chat': {
      const r = ws.room;
      const text = String(msg.text || '').slice(0, 120).trim();
      if (!r || !text) return;
      return broadcast(r, { t: 'chat', seat: ws.seat, name: ws.user.name, text });
    }
    case 'start': {
      const r = ws.room;
      if (!r || r.status !== 'waiting' || ws.seat !== r.hostSeat) return;
      const notReady = r.seats.filter((s, i) => s.kind === 'human' && i !== r.hostSeat && !s.ready);
      if (notReady.length) return send(ws, { t: 'error', code: 'not_ready', msg: 'ยังมีผู้เล่นที่ไม่กดพร้อม' });
      r.seats.forEach((s, i) => { if (s.kind === 'open') r.seats[i] = { kind: 'bot', name: 'บอท ' + (i + 1) }; });
      r.status = 'playing';
      r.seed = crypto.randomInt(1, 2 ** 31 - 1);
      r.log = [];
      r.hashes = new Map();
      r.startedAt = Date.now();
      for (let i = 0; i < r.seats.length; i++) if (r.seats[i].ws) send(r.seats[i].ws, gameStartMsg(r, i));
      lobbyBroadcast();
      return;
    }
    case 'cmd': {
      const r = ws.room;
      if (!r || r.status !== 'playing') return;
      const seat = Number(msg.seat);
      const s = r.seats[seat];
      if (!s) return;
      // สิทธิ์: ที่นั่งคนจริงที่ต่ออยู่ = เจ้าของที่นั่งเท่านั้น, นอกนั้น (บอท/หลุด) = ผู้คุมบอท
      const humanInControl = s.kind === 'human' && !s.botControl;
      const allowed = humanInControl ? ws.seat === seat : ws.seat === r.hostSeat;
      if (!allowed) return send(ws, { t: 'cmd_reject', at: msg.at, reason: 'not_your_seat' });
      if (Number(msg.at) !== r.log.length) return send(ws, { t: 'cmd_reject', at: msg.at, reason: 'stale' });
      if (r.log.length >= MAX_LOG) return;
      const c = { i: r.log.length, seat, a: String(msg.a || ''), args: Array.isArray(msg.args) ? msg.args.slice(0, 8) : [] };
      r.log.push(c);
      broadcast(r, { t: 'cmd', ...c });
      return;
    }
    case 'hash': {
      // ตรวจว่าทุกเครื่องได้สถานะเกมตรงกัน
      const r = ws.room;
      if (!r || r.status !== 'playing') return;
      const i = Number(msg.i);
      let m = r.hashes.get(i);
      if (!m) { m = new Map(); r.hashes.set(i, m); }
      m.set(ws.seat, String(msg.h));
      const vals = new Set(m.values());
      if (vals.size > 1 && !m.reportedDesync) {
        m.reportedDesync = true;
        console.warn(`[${r.code}] สถานะไม่ตรงกันที่คำสั่ง ${i}:`, [...m.entries()]);
        broadcast(r, { t: 'desync', i });
      }
      if (r.hashes.size > 50) r.hashes.delete(r.hashes.keys().next().value);
      return;
    }
    case 'game_over': {
      const r = ws.room;
      if (!r || r.status !== 'playing' || ws.seat !== r.hostSeat || r.reported) return;
      r.reported = true;
      const winners = new Set((msg.winners || []).map(Number));
      for (let i = 0; i < r.seats.length; i++) {
        const s = r.seats[i];
        if (s.kind === 'human') await store.addResult(s.key, winners.has(i));
      }
      r.status = 'finished';
      for (const s of r.seats) if (s.ws) {
        const u = await store.getUser(s.ws.user.key);
        send(s.ws, { t: 'me', name: u.display, games: u.games, wins: u.wins });
      }
      // ห้องที่จบแล้ว ลบหลังผ่านไปสักพัก
      setTimeout(() => rooms.delete(r.code), 5 * 60 * 1000);
      return;
    }
    default:
      return send(ws, { t: 'error', code: 'unknown', msg: 'ไม่รู้จักคำสั่ง ' + t });
  }
}

// ---------------- เซิร์ฟเวอร์ HTTP + WebSocket ----------------
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end(`ศึกชิงแผ่นดิน server ok · ห้อง ${rooms.size} · ออนไลน์ ${clients.size}\n`);
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
wss.on('connection', (ws, req) => {
  ws.ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  ws.isAlive = true;
  ws.room = null; ws.seat = -1; ws.user = null; ws.lobby = false;
  let chain = Promise.resolve();   // ประมวลผลข้อความตามลำดับ
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(String(data)); } catch (e) { return send(ws, { t: 'error', code: 'json', msg: 'ข้อความเสีย' }); }
    if (!msg || typeof msg.t !== 'string') return;
    chain = chain.then(() => handle(ws, msg)).catch(e => {
      console.error('error', e);
      send(ws, { t: 'error', code: 'server', msg: 'เซิร์ฟเวอร์ขัดข้อง' });
    });
  });
  ws.on('close', () => {
    chain.then(() => {
      leaveRoom(ws, false);
      clients.delete(ws);
    });
  });
});

// ตัดการเชื่อมต่อที่เงียบหาย + กันโฮสต์ปิดการเชื่อมต่อที่ว่างนาน
const pinger = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

store.init().then(() => {
  server.listen(PORT, () => console.log(`ศึกชิงแผ่นดิน server พร้อมที่พอร์ต ${PORT}`));
}).catch(e => { console.error('เชื่อมฐานข้อมูลไม่ได้', e); process.exit(1); });

function shutdown() {
  clearInterval(pinger);
  try { store.flush(); } catch (e) {}
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

module.exports = { server };
