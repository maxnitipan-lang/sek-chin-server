// ที่เก็บข้อมูลบัญชีผู้เล่น
// - ถ้ามีตัวแปร DATABASE_URL (Postgres เช่น Neon / Render / Supabase) จะใช้ฐานข้อมูลนั้น
// - ถ้าไม่มี จะเก็บเป็นไฟล์ JSON (เหมาะกับรันบนเครื่องตัวเองหรือทดสอบ)
'use strict';
const fs = require('fs');
const path = require('path');

class FileStore {
  constructor(file) {
    this.file = file;
    this.data = { users: {}, sessions: {} };
    try {
      if (fs.existsSync(file)) this.data = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch (e) {
      console.error('อ่านไฟล์ข้อมูลไม่ได้ เริ่มใหม่:', e.message);
    }
    this.data.users = this.data.users || {};
    this.data.sessions = this.data.sessions || {};
    this._timer = null;
  }
  async init() {}
  _save() {
    if (this._timer) return;
    this._timer = setTimeout(() => {
      this._timer = null;
      const tmp = this.file + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(this.data));
      fs.renameSync(tmp, this.file);
    }, 200);
  }
  flush() {
    if (this._timer) { clearTimeout(this._timer); this._timer = null; }
    fs.writeFileSync(this.file, JSON.stringify(this.data));
  }
  async getUser(key) { return this.data.users[key] || null; }
  async createUser(u) {
    if (this.data.users[u.key]) return false;
    this.data.users[u.key] = { ...u, games: 0, wins: 0, created: Date.now() };
    this._save();
    return true;
  }
  async addResult(key, won) {
    const u = this.data.users[key];
    if (!u) return;
    u.games += 1;
    if (won) u.wins += 1;
    this._save();
  }
  async saveSession(token, key, expires) {
    this.data.sessions[token] = { key, expires };
    this._save();
  }
  async getSession(token) {
    const s = this.data.sessions[token];
    if (!s) return null;
    if (s.expires < Date.now()) { delete this.data.sessions[token]; this._save(); return null; }
    return s;
  }
  async deleteSession(token) { delete this.data.sessions[token]; this._save(); }
  async topPlayers(limit) {
    return Object.values(this.data.users)
      .sort((a, b) => b.wins - a.wins || a.games - b.games)
      .slice(0, limit)
      .map(u => ({ name: u.display, wins: u.wins, games: u.games }));
  }
}

class PgStore {
  constructor(url) {
    const { Pool } = require('pg');
    const ssl = /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: false };
    this.pool = new Pool({ connectionString: url, ssl, max: 5 });
  }
  async init() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS users (
      key TEXT PRIMARY KEY, display TEXT NOT NULL, salt TEXT NOT NULL, hash TEXT NOT NULL,
      games INTEGER NOT NULL DEFAULT 0, wins INTEGER NOT NULL DEFAULT 0,
      created BIGINT NOT NULL)`);
    await this.pool.query(`CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY, key TEXT NOT NULL, expires BIGINT NOT NULL)`);
  }
  flush() {}
  async getUser(key) {
    const r = await this.pool.query('SELECT * FROM users WHERE key=$1', [key]);
    if (!r.rows.length) return null;
    const u = r.rows[0];
    return { ...u, games: Number(u.games), wins: Number(u.wins) };
  }
  async createUser(u) {
    const r = await this.pool.query(
      'INSERT INTO users(key, display, salt, hash, created) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING',
      [u.key, u.display, u.salt, u.hash, Date.now()]);
    return r.rowCount === 1;
  }
  async addResult(key, won) {
    await this.pool.query('UPDATE users SET games=games+1, wins=wins+$2 WHERE key=$1', [key, won ? 1 : 0]);
  }
  async saveSession(token, key, expires) {
    await this.pool.query('INSERT INTO sessions(token, key, expires) VALUES($1,$2,$3)', [token, key, expires]);
  }
  async getSession(token) {
    const r = await this.pool.query('SELECT * FROM sessions WHERE token=$1', [token]);
    if (!r.rows.length) return null;
    const s = r.rows[0];
    if (Number(s.expires) < Date.now()) { await this.deleteSession(token); return null; }
    return { key: s.key, expires: Number(s.expires) };
  }
  async deleteSession(token) { await this.pool.query('DELETE FROM sessions WHERE token=$1', [token]); }
  async topPlayers(limit) {
    const r = await this.pool.query('SELECT display, wins, games FROM users ORDER BY wins DESC, games ASC LIMIT $1', [limit]);
    return r.rows.map(u => ({ name: u.display, wins: Number(u.wins), games: Number(u.games) }));
  }
}

function makeStore() {
  if (process.env.DATABASE_URL) {
    console.log('ใช้ฐานข้อมูล Postgres จาก DATABASE_URL');
    return new PgStore(process.env.DATABASE_URL);
  }
  const file = process.env.DATA_FILE || path.join(__dirname, 'data.json');
  console.log('เก็บข้อมูลในไฟล์', file);
  return new FileStore(file);
}

module.exports = { makeStore, FileStore };
