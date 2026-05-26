// ════════════════════════════════════════════════════════════════
//  REALM ETERNAL — Serveur Multijoueur
//  Local  : npm install && node server.js
//  Railway: déploiement automatique via GitHub
// ════════════════════════════════════════════════════════════════
const express = require('express');
const http    = require('http');
const { Server } = require('socket.io');
const path    = require('path');
const crypto  = require('crypto');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin: '*', methods: ['GET','POST'] },
  pingTimeout:  20000,
  pingInterval:  5000,
  transports: ['websocket', 'polling'],
});

const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0'; // requis pour Railway

// Servir le jeu depuis /public
// Sert depuis /public si existe, sinon depuis la racine
const fs = require('fs');
const publicDir = path.join(__dirname, 'public');
const rootHtml = path.join(__dirname, 'index.html');
if(fs.existsSync(publicDir)){
  app.use(express.static(publicDir));
  app.get('/', (_req, res) => res.sendFile(path.join(publicDir, 'index.html')));
} else {
  app.use(express.static(__dirname));
  app.get('/', (_req, res) => res.sendFile(rootHtml));
}
app.get('/health', (_req, res) =>
  res.json({ ok: true, rooms: rooms.size, players: io.engine.clientsCount })
);

// ────────────────────────────────────────────────────────────────
const rooms = new Map();

function genCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}
function getRoomOf(socketId) {
  for (const [code, room] of rooms)
    if (room.players.has(socketId)) return { code, room };
  return null;
}
class RoomState {
  constructor(hostId) {
    this.hostId = hostId;
    this.players = new Map();
    this.enemies = []; this.enemyBullets = [];
    this.lootDrops = []; this.bossRef = null;
    this.created = Date.now();
  }
}

io.on('connection', (socket) => {
  console.log('[+]', socket.id, '| clients:', io.engine.clientsCount);

  socket.on('host', ({ classId, snap }) => {
    const ex = getRoomOf(socket.id);
    if (ex) leaveRoom(socket, ex.code);
    const code = genCode();
    const room = new RoomState(socket.id);
    room.players.set(socket.id, { ...(snap||{}), id: socket.id, classId, isHost: true });
    rooms.set(code, room);
    socket.join(code);
    socket.emit('hosted', { code, playerId: socket.id });
    console.log('[room]', code, 'créée');
  });

  socket.on('join', ({ code, classId, playerSnap }) => {
    code = (code||'').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room)                  return socket.emit('join_err', 'Room introuvable');
    if (room.players.size >= 2) return socket.emit('join_err', 'Room pleine (max 2)');
    room.players.set(socket.id, { ...(playerSnap||{}), id: socket.id, classId, isHost: false });
    socket.join(code);
    socket.emit('joined', {
      code, playerId: socket.id, hostId: room.hostId,
      hostSnap: room.players.get(room.hostId),
      enemies: room.enemies, lootDrops: room.lootDrops,
    });
    io.to(room.hostId).emit('partner_joined', {
      partnerId: socket.id,
      partnerSnap: room.players.get(socket.id),
    });
    console.log('[room]', socket.id, 'rejoint', code);
  });

  socket.on('player_state', (snap) => {
    const f = getRoomOf(socket.id); if (!f) return;
    const s = f.room.players.get(socket.id);
    if (s) Object.assign(s, snap);
    socket.to(f.code).emit('partner_state', { id: socket.id, ...snap });
  });

  socket.on('player_shoot', (data) => {
    const f = getRoomOf(socket.id); if (!f) return;
    socket.to(f.code).emit('partner_shoot', { shooterId: socket.id, ...data });
  });

  socket.on('enemy_state', ({ enemies, enemyBullets, lootDrops, bossRef }) => {
    const f = getRoomOf(socket.id);
    if (!f || f.room.hostId !== socket.id) return;
    Object.assign(f.room, { enemies, enemyBullets, lootDrops, bossRef });
    socket.to(f.code).emit('world_state', { enemies, enemyBullets, lootDrops, bossRef });
  });

  socket.on('enemy_hit', ({ enemyIdx, dmg, isCrit }) => {
    const f = getRoomOf(socket.id); if (!f) return;
    const e = f.room.enemies[enemyIdx]; if (!e) return;
    e.hp -= dmg;
    io.to(f.code).emit('enemy_damaged', { enemyIdx, dmg, isCrit, hp: e.hp, maxHp: e.maxHp, dead: e.hp <= 0 });
  });

  socket.on('loot_pickup', ({ lootId }) => {
    const f = getRoomOf(socket.id); if (!f) return;
    f.room.lootDrops = f.room.lootDrops.filter(l => l.uid !== lootId);
    socket.to(f.code).emit('loot_removed', { lootId });
  });

  socket.on('chat', ({ msg }) => {
    const f = getRoomOf(socket.id); if (!f) return;
    io.to(f.code).emit('chat', { id: socket.id, msg: String(msg||'').slice(0,200) });
  });

  socket.on('boss_killed', (data) => {
    const f = getRoomOf(socket.id); if (!f) return;
    io.to(f.code).emit('boss_killed', { killedBy: socket.id, ...data });
  });

  socket.on('xp_event', ({ amount }) => {
    const f = getRoomOf(socket.id); if (!f) return;
    socket.to(f.code).emit('xp_event', { amount, fromId: socket.id });
  });

  socket.on('disconnect', (reason) => {
    console.log('[-]', socket.id, reason);
    const f = getRoomOf(socket.id);
    if (f) leaveRoom(socket, f.code);
  });
});

function leaveRoom(socket, code) {
  const room = rooms.get(code); if (!room) return;
  room.players.delete(socket.id);
  socket.leave(code);
  io.to(code).emit('partner_left', { id: socket.id });
  if (room.players.size === 0 || socket.id === room.hostId) {
    rooms.delete(code);
    io.to(code).emit('room_closed');
    console.log('[room]', code, 'fermée');
  }
}

setInterval(() => {
  const limit = Date.now() - 3_600_000;
  for (const [code, room] of rooms)
    if (room.created < limit) { rooms.delete(code); io.to(code).emit('room_closed'); }
}, 600_000);

server.listen(PORT, HOST, () => {
  console.log(`\n🎮 REALM ETERNAL — http://localhost:${PORT}\n`);
});
