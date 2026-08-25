/**
 * PostgreSQL 数据库层 — 房间与世界方块持久化
 * 需要环境变量：DATABASE_URL 或 PGHOST/PGUSER/PGPASSWORD
 * .env 文件自动加载（dotenv）
 */

import 'dotenv/config';
import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // 本地开发可回退到默认配置
  ...(process.env.DATABASE_URL ? {} : {
    host: process.env.PGHOST || 'localhost',
    port: parseInt(process.env.PGPORT || '5432'),
    database: process.env.PGDATABASE || 'voxelworld',
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'password',
  }),
});

/** 初始化数据库表（幂等） */
export async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS rooms (
        id UUID PRIMARY KEY,
        name TEXT NOT NULL,
        max_players INT NOT NULL DEFAULT 4,
        seed INT NOT NULL,
        password TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    // 兼容旧表：补加 password 列（已存在则跳过）
    await client.query(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS password TEXT NOT NULL DEFAULT '';`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS world_blocks (
        room_id UUID NOT NULL,
        x INT NOT NULL,
        y INT NOT NULL,
        z INT NOT NULL,
        block_type INT NOT NULL,
        player_id TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (room_id, x, y, z)
      );
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_world_blocks_room ON world_blocks(room_id);
    `);
    console.log('[db] tables initialized');
  } finally {
    client.release();
  }
}

/** 保存房间元信息（含密码） */
export async function saveRoom(room) {
  await pool.query(
    `INSERT INTO rooms (id, name, max_players, seed, password, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, to_timestamp($6/1000.0), NOW())
     ON CONFLICT (id) DO UPDATE SET
       name = EXCLUDED.name,
       max_players = EXCLUDED.max_players,
       seed = EXCLUDED.seed,
       password = EXCLUDED.password,
       updated_at = NOW()`,
    [room.id, room.name, room.maxPlayers, room.worldSeed, room.password || '', room.createdAt]
  );
}

/** 加载房间元信息（若无则 null） */
export async function loadRoom(roomId) {
  const { rows } = await pool.query(
    'SELECT * FROM rooms WHERE id = $1',
    [roomId]
  );
  return rows[0] || null;
}

/** 保存单个方块改动（UPSERT） */
export async function saveBlock(roomId, x, y, z, blockType, playerId) {
  await pool.query(
    `INSERT INTO world_blocks (room_id, x, y, z, block_type, player_id, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, NOW())
     ON CONFLICT (room_id, x, y, z) DO UPDATE SET
       block_type = EXCLUDED.block_type,
       player_id = EXCLUDED.player_id,
       updated_at = NOW()`,
    [roomId, x, y, z, blockType, playerId]
  );
}

/** 批量保存方块改动（性能优化） */
export async function saveBlocksBatch(roomId, blocks) {
  if (!blocks || blocks.length === 0) return;
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // 使用 unnest 批量插入（最高效）
    const xs = blocks.map(b => b.x);
    const ys = blocks.map(b => b.y);
    const zs = blocks.map(b => b.z);
    const types = blocks.map(b => b.type);
    const pids = blocks.map(b => b.playerId || null);
    await client.query(
      `INSERT INTO world_blocks (room_id, x, y, z, block_type, player_id, updated_at)
       SELECT $1, unnest($2::int[]), unnest($3::int[]), unnest($4::int[]), unnest($5::int[]), unnest($6::text[]), NOW()
       ON CONFLICT (room_id, x, y, z) DO UPDATE SET
         block_type = EXCLUDED.block_type,
         player_id = EXCLUDED.player_id,
         updated_at = NOW()`,
      [roomId, xs, ys, zs, types, pids]
    );
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

/** 加载房间的全部方块改动 */
export async function loadBlocks(roomId) {
  const { rows } = await pool.query(
    'SELECT x, y, z, block_type as type FROM world_blocks WHERE room_id = $1',
    [roomId]
  );
  return rows;
}

/** 删除房间及其方块 */
export async function deleteRoom(roomId) {
  await pool.query('DELETE FROM world_blocks WHERE room_id = $1', [roomId]);
  await pool.query('DELETE FROM rooms WHERE id = $1', [roomId]);
}

/** 列出所有房间（不泄露密码，只返回 hasPassword 标记） */
export async function listRooms() {
  const { rows } = await pool.query(
    `SELECT id, name, max_players, seed, (password <> '') AS has_password,
            created_at, updated_at
     FROM rooms ORDER BY updated_at DESC`
  );
  return rows;
}

export { pool };
