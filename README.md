# 像素方块世界 — 服务器部署指南

## 环境要求

- Node.js 18+（已验证 Node.js v22）
- PostgreSQL 14+（腾讯云 PostgreSQL 兼容）

## 快速启动

### 1. 安装依赖

```bash
cd server
npm install
```

### 2. 配置数据库

在 `server/.env` 文件中写入你的 PostgreSQL 连接信息：

```env
DATABASE_URL=postgresql://user:password@host:port/database?sslmode=require
```

或者分别设置：

```env
PGHOST=your-pg-host.com
PGPORT=5432
PGDATABASE=voxelworld
PGUSER=youruser
PGPASSWORD=yourpassword
```

### 3. 启动服务器

```bash
npm start
```

服务器默认监听 `3001` 端口：
- WebSocket: `ws://localhost:3001/ws?room=<roomId>&name=<playerName>`
- HTTP API: `http://localhost:3001/rooms`

## 前端配置

1. 打开游戏 `index.html`，进入联机模式
2. 在"服务器"输入框填入你的服务器地址，例如：
   - 本地测试：`ws://localhost:3001`
   - 远程服务器：`wss://your-server.com`
3. 创建房间或加入已有房间

## 功能说明

### 单机模式
- 3 个存档槽位，进入游戏时选择
- 支持新建、加载、删除存档（删除需确认）
- 自动存档绑定当前活跃槽位
- 支持导出/导入存档码

### 联机模式
- 创建房间（2-4人）
- 通过 HTTP API 列出并加入房间
- 实时玩家位置同步
- 方块改动广播（挖/放方块所有玩家可见）
- 世界状态持久化到 PostgreSQL（房间恢复、方块改动不丢失）
- 聊天消息广播
- 心跳检测 + 自动断线处理

## 数据库表结构

服务器启动时会自动创建以下表：

- `rooms` — 房间元信息（ID、名称、人数上限、种子、时间戳）
- `world_blocks` — 方块改动记录（按房间+坐标联合主键，UPSERT）

## 腾讯云 PostgreSQL 配置建议

1. 在腾讯云控制台创建 PostgreSQL 实例
2. 开启外网访问（安全组放行 5432 端口）
3. 创建数据库 `voxelworld`
4. 复制连接字符串到 `.env`
5. 确保 Node.js 服务器也能访问该数据库（如服务器和数据库在同一 VPC）

## 故障排查

| 问题 | 排查方向 |
|---|---|
| 前端提示"无法连接到服务器" | 检查服务器是否启动、防火墙是否放行端口、ws/wss 协议是否匹配 |
| 数据库连接失败 | 检查 DATABASE_URL 或 PGHOST/PGUSER/PGPASSWORD 配置、SSL 模式 |
| 方块改动不同步 | 检查 WebSocket 是否连通、浏览器控制台 Network 标签 |
| 房间列表为空 | 服务器刚启动时没有房间，需要玩家先创建一个 |
