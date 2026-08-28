/**
 * Voxel World WebSocket Game Server
 * 支持：房间管理、玩家状态同步、方块改动广播、世界持久化（PostgreSQL）
 * 数据库连接失败时自动降级为内存模式（日志有警告）
 */

import { WebSocketServer } from 'ws';
import { createServer } from 'http';
import { randomUUID } from 'crypto';

let db;
try {
  db = await import('./db.js');
  await db.initDB();
} catch (e) {
  console.warn('[server] PostgreSQL 连接失败，切换到内存模式（重启后数据会丢失）:', e.message);
  const roomsMap = new Map();
  const blocksMap = new Map();
  db = {
    pool: null,
    initDB: async () => {},
    saveRoom: async (room) => { roomsMap.set(room.id, room); },
    loadRoom: async (id) => roomsMap.get(id) || null,
    saveBlock: async (rid, x, y, z, t, pid) => {
      blocksMap.set(`${rid}|${x}|${y}|${z}`, { x, y, z, type: t, player_id: pid });
    },
    saveBlocksBatch: async () => {},
    loadBlocks: async (rid) => {
      const out = [];
      for (const [k, v] of blocksMap) {
        if (k.startsWith(rid + '|')) out.push(v);
      }
      return out;
    },
    listRooms: async () => Array.from(roomsMap.values()).map(r => ({
      id: r.id,
      name: r.name,
      max_players: r.maxPlayers,
      seed: r.worldSeed,
      has_password: !!r.password,
      updated_at: new Date(),
    })),
    roomNameExists: async (name) => {
      for (const r of roomsMap.values()) { if (r.name === name) return true; }
      return false;
    },
    deleteRoom: async (id) => { roomsMap.delete(id); blocksMap.delete(id); },
  };
}

const {
  saveRoom, loadRoom, saveBlock, saveBlocksBatch,
  loadBlocks, listRooms, deleteRoom, roomNameExists,
} = db;

// 纯函数：依据世界种子确定性推导出生点 (x,z)。
// 与客户端 voxel.js 的 computeSpawnFromSeed 算法保持一致（不含地形安全检查，
// 由客户端 findSafeSpawnPos 兜底），确保「每个种子只有一个出生坐标」。
function computeSpawnFromSeed(seed) {
  const h1 = ((seed * 73856093) ^ (seed >>> 13)) >>> 0;
  const h2 = ((seed * 19349663) ^ (seed >>> 7)) >>> 0;
  const r = 32 + (h1 % 64);
  const a = (h2 % 360) * Math.PI / 180;
  let x = 8 + Math.round(Math.cos(a) * r);
  let z = 8 + Math.round(Math.sin(a) * r);
  x = Math.max(-128, Math.min(128, x));
  z = Math.max(-128, Math.min(128, z));
  return { x, z };
}

// 出生点 Y 哨兵：服务端无地形数据，y<0 表示「客户端需按地形计算安全出生高度」。
const SPAWN_Y_SENTINEL = -1;

const PORT = process.env.PORT || 3001;
const MAX_PLAYERS_PER_ROOM = 4;
const HEARTBEAT_INTERVAL = 30000;
const HEARTBEAT_TIMEOUT = 60000;

// 内存中的房间与玩家状态
const rooms = new Map(); // roomId -> Room

class Room {
  constructor(id, options = {}) {
    this.id = id;
    this.name = options.name || `Room ${id.slice(0, 6)}`;
    this.maxPlayers = options.maxPlayers || MAX_PLAYERS_PER_ROOM;
    this.players = new Map(); // playerId -> PlayerState
    this.worldSeed = options.seed || Math.floor(Math.random() * 100000);
    this.password = options.password || '';
    this.createdAt = Date.now();
    this.lastActivity = Date.now();
  }

  checkPassword(pwd) {
    if (!this.password) return true;
    return pwd === this.password;
  }

  addPlayer(ws, name) {
    if (this.players.size >= this.maxPlayers) return null;
    const playerId = randomUUID();
    // v18: 出生点从种子确定性推导，而非写死 (8,8)；y=-1 哨兵交由客户端按地形求安全高度
    const sp = computeSpawnFromSeed(this.worldSeed);
    // v21: 按加入顺序在种子出生点周围扇形偏移，避免多人玩家叠在同一坐标互相看不见
    const idx = this.players.size; // 0-based，第一位玩家 idx=0 落在种子点
    const angle = idx * (Math.PI * 2 / Math.max(1, MAX_PLAYERS_PER_ROOM));
    // v37: 半径有界（贴着固定种子出生点排成小圈），不再随人数无限外扩，避免"出生点跟随玩家散开"
    const radius = idx === 0 ? 0 : 2 + (idx % 4) * 1.2;
    let x = Math.round(sp.x + Math.cos(angle) * radius);
    let z = Math.round(sp.z + Math.sin(angle) * radius);
    x = Math.max(-128, Math.min(128, x));
    z = Math.max(-128, Math.min(128, z));
    const state = {
      id: playerId,
      name: name || `Player ${idx + 1}`,
      ws,
      position: { x, y: SPAWN_Y_SENTINEL, z },
      spawnX: x,
      spawnZ: z,
      yaw: 0,
      pitch: 0,
      inBoat: false,
      lastPing: Date.now(),
      joinedAt: Date.now(),
    };
    this.players.set(playerId, state);
    this.lastActivity = Date.now();
    return state;
  }

  removePlayer(playerId) {
    this.players.delete(playerId);
    this.lastActivity = Date.now();
    if (this.players.size === 0) {
      setTimeout(() => {
        if (this.players.size === 0) rooms.delete(this.id);
      }, 10 * 60 * 1000);
    }
  }

  broadcast(msg, excludePlayerId) {
    const payload = JSON.stringify(msg);
    for (const [pid, p] of this.players) {
      if (pid === excludePlayerId) continue;
      if (p.ws.readyState === 1) {
        try { p.ws.send(payload); } catch (e) {}
      }
    }
  }

  broadcastToAll(msg) { this.broadcast(msg, null); }

  async setBlock(x, y, z, type, playerId) {
    try { await saveBlock(this.id, x, y, z, type, playerId); }
    catch (e) { console.error('[db] saveBlock failed:', e.message); }
    this.broadcast({ type: 'blockUpdate', x, y, z, blockType: type, playerId });
  }

  getPlayersList() {
    return Array.from(this.players.values()).map(p => ({
      playerId: p.id, name: p.name, position: p.position, yaw: p.yaw, pitch: p.pitch, inBoat: !!p.inBoat,
    }));
  }
}

// HTTP + WS 共用服务器
const httpServer = createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, mode: db.pool ? 'postgres' : 'memory' }));
    return;
  }

  if (url.pathname === '/rooms' && req.method === 'GET') {
    listRooms().then(rows => {
      const list = rows.map(r => {
        const memRoom = rooms.get(r.id);
        return {
          id: r.id,
          name: r.name,
          players: memRoom ? memRoom.players.size : 0,
          maxPlayers: r.max_players,
          seed: r.seed,
          hasPassword: !!r.has_password,
        };
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(list));
    }).catch(e => { console.error('[http] list rooms error:', e); res.writeHead(500); res.end('Internal Error'); });
    return;
  }

  if (url.pathname === '/rooms' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      (async () => {
        try {
          const opts = body ? JSON.parse(body) : {};
          const password = String(opts.password || '');
          if (!password) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'password required' }));
            return;
          }
          if (password.length > 20) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'password too long (max 20)' }));
            return;
          }
          // v21: 重名校验 —— 每个房间名唯一对应一个房间
          if (opts.name) {
            try {
              const exists = await roomNameExists(String(opts.name));
              if (exists) {
                res.writeHead(409, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: '房间名已存在，请换一个名字' }));
                return;
              }
            } catch (e) { /* 查重失败不阻断创建 */ }
          }
          opts.password = password;
          const roomId = randomUUID();
          const room = new Room(roomId, opts);
          rooms.set(roomId, room);
          await saveRoom(room);
          console.log(`[server] room created: ${room.name} (${roomId}) pwd=***`);
          res.writeHead(201, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ id: roomId, name: room.name, seed: room.worldSeed }));
        } catch (e) { console.error('[http] create room error:', e); res.writeHead(400); res.end('Bad Request'); }
      })();
    });
    return;
  }

  // v21: 删除房间（及其方块数据）
  if (url.pathname.startsWith('/rooms/') && req.method === 'DELETE') {
    const roomId = url.pathname.split('/').pop();
    (async () => {
      try { await deleteRoom(roomId); } catch (e) { console.error('[http] delete room error:', e); }
      const room = rooms.get(roomId);
      if (room) {
        room.broadcastToAll({ type: 'roomDeleted' });
        rooms.delete(roomId);
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    })();
    return;
  }

  res.writeHead(404); res.end('Not Found');
});

const wss = new WebSocketServer({ server: httpServer });

function send(ws, msg) {
  if (ws.readyState === 1) { try { ws.send(JSON.stringify(msg)); } catch (e) {} }
}

wss.on('connection', async (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const roomId = url.searchParams.get('room');
  const playerName = url.searchParams.get('name') || 'Player';
  const password = url.searchParams.get('password') || '';

  if (!roomId) { send(ws, { type: 'error', message: 'Room ID required' }); ws.close(); return; }

  let room = rooms.get(roomId);
  if (!room) {
    const dbRoom = await loadRoom(roomId);
    if (dbRoom) {
      room = new Room(dbRoom.id, {
        name: dbRoom.name,
        maxPlayers: dbRoom.max_players,
        seed: dbRoom.seed,
        password: dbRoom.password || '',
      });
      rooms.set(roomId, room);
    }
  }

  if (!room) { send(ws, { type: 'error', message: 'Room not found' }); ws.close(); return; }
  if (!room.checkPassword(password)) { send(ws, { type: 'error', message: '密码错误' }); ws.close(); return; }

  const player = room.addPlayer(ws, playerName);
  if (!player) { send(ws, { type: 'error', message: 'Room is full' }); ws.close(); return; }

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    const p = room.players.get(player.id);
    if (!p) return;

    switch (msg.type) {
      case 'playerState':
        p.position = msg.position;
        p.yaw = msg.yaw;
        p.pitch = msg.pitch;
        p.inBoat = !!msg.inBoat;
        // v38: 转发护甲，让换装在其他客户端可见
        if (Array.isArray(msg.armor)) p.armor = msg.armor;
        room.broadcast({ type: 'playerUpdate', playerId: player.id, position: msg.position, yaw: msg.yaw, pitch: msg.pitch, inBoat: p.inBoat, armor: p.armor || null }, player.id);
        break;
      // v38: 玩家互击（只做击退/受击反馈，不掉血）
      case 'pvpHit': {
        const targetId = String(msg.targetId || '');
        if (!targetId || !room.players.has(targetId)) break;
        const dir = msg.knockback || null;
        room.broadcast({ type: 'pvpHit', fromId: player.id, playerId: player.id, targetId, knockback: dir }, player.id);
        break;
      }
      case 'setBlock':
        await room.setBlock(msg.x, msg.y, msg.z, msg.blockType, player.id);
        break;
      case 'requestWorld': {
        const delta = await loadBlocks(roomId);
        send(ws, { type: 'worldInit', seed: room.worldSeed, delta });
        break;
      }
      case 'chat':
        room.broadcastToAll({ type: 'chat', playerId: player.id, name: p.name, text: String(msg.text || '').slice(0, 200) });
        break;
      case 'ping':
        p.lastPing = Date.now();
        send(ws, { type: 'pong', ts: msg.ts });
        break;
    }
  });

  ws.on('close', () => {
    room.removePlayer(player.id);
    room.broadcastToAll({ type: 'playerLeave', playerId: player.id });
  });

  // 发送初始化信息
  const delta = await loadBlocks(roomId);
  send(ws, {
    type: 'init',
    playerId: player.id,
    roomId: room.id,
    roomName: room.name,
    seed: room.worldSeed,
    // v37: 下发本玩家专属出生点（固定种子点 + 有界偏移），不再使用随移动变化的 player.position
    mySpawn: { x: player.spawnX, z: player.spawnZ },
    players: room.getPlayersList().filter(p => p.id !== player.id),
    delta,
  });

  // 通知其他人
  room.broadcastToAll({ type: 'playerJoin', playerId: player.id, name: player.name });
});

// 心跳清理
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    for (const [pid, p] of room.players) {
      if (now - p.lastPing > HEARTBEAT_TIMEOUT) {
        console.log(`[hb] player ${pid} timeout`);
        p.ws.terminate();
        room.removePlayer(pid);
        room.broadcastToAll({ type: 'playerLeave', playerId: pid });
      }
    }
  }
}, HEARTBEAT_INTERVAL);

httpServer.listen(PORT, () => {
  console.log(`[server] Voxel World Server running on port ${PORT}`);
  console.log(`[server] WebSocket endpoint: ws://localhost:${PORT}/ws?room=<roomId>&name=<playerName>`);
  console.log(`[server] HTTP API: http://localhost:${PORT}/rooms`);
});
