// YT Music Jam - relay server + phone web player
// Run: npm install && node server.js   (PORT env var optional, default 8787)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const PAGE = fs.existsSync(path.join(__dirname, 'public', 'jam.html'))
  ? path.join(__dirname, 'public', 'jam.html')
  : path.join(__dirname, 'jam.html');

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') return res.end('ok');
  if (url === '/' || url.startsWith('/r/')) {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    return res.end(fs.readFileSync(PAGE));
  }
  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> { clients:Set<ws>, host, state, queue:[] }
let nextItemId = 1;

const send = (ws, msg) => ws.readyState === 1 && ws.send(JSON.stringify(msg));
const broadcast = (room, msg, except) => {
  for (const c of room.clients) if (c !== except) send(c, msg);
};
// "elapsed" lets clients extrapolate the position without clock sync
const snapshot = (room) =>
  room.state ? { ...room.state, elapsed: Date.now() - room.state.serverTs } : null;
const members = (room) =>
  [...room.clients].map((c) => ({ name: c.name, host: c === room.host }));
const sendQueue = (room) => broadcast(room, { type: 'queue', queue: room.queue });
const validId = (id) => typeof id === 'string' && /^[\w-]{11}$/.test(id);
const clip = (s, n) => (typeof s === 'string' ? s.trim().slice(0, n) : '');

// Fill in title/artist for pasted links using YouTube's public oEmbed endpoint
async function enrich(item, room) {
  try {
    const u = 'https://www.youtube.com/oembed?format=json&url=' +
      encodeURIComponent('https://www.youtube.com/watch?v=' + item.videoId);
    const r = await fetch(u);
    if (r.ok) {
      const j = await r.json();
      item.title = clip(j.title, 120);
      item.artist = item.artist || clip((j.author_name || '').replace(/ - Topic$/, ''), 80);
    }
  } catch {}
  if (!item.title) item.title = 'Unknown song';
  sendQueue(room);
}

function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  room.clients.delete(ws);
  if (room.host === ws) {
    room.host = [...room.clients][0] || null;
    if (room.host) send(room.host, { type: 'host' });
  }
  if (!room.clients.size) rooms.delete(ws.room);
  else broadcast(room, { type: 'members', members: members(room) });
  ws.room = null;
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      leave(ws);
      const roomId = clip(String(m.room || ''), 40);
      if (!roomId) return;
      let room = rooms.get(roomId);
      if (!room) {
        room = { clients: new Set(), host: ws, state: null, queue: [] };
        rooms.set(roomId, room);
      }
      ws.room = roomId;
      ws.name = clip(String(m.name || ''), 30) || 'Guest';
      room.clients.add(ws);
      send(ws, { type: 'welcome', isHost: room.host === ws, state: snapshot(room), queue: room.queue });
      broadcast(room, { type: 'members', members: members(room) });
      console.log(`${ws.name} joined ${roomId} (${room.clients.size} in room)`);
      return;
    }

    const room = rooms.get(ws.room);
    if (!room) return;

    switch (m.type) {
      case 'ping':
        send(ws, { type: 'pong' });
        break;

      case 'state':
        if (!validId(m.videoId)) return;
        room.state = {
          videoId: m.videoId,
          position: Number(m.position) || 0,
          playing: !!m.playing,
          serverTs: Date.now(),
          by: ws.name,
        };
        broadcast(room, { type: 'state', state: snapshot(room) }, ws);
        break;

      case 'queue_add': {
        if (!validId(m.videoId) || room.queue.length >= 100) return;
        const item = {
          id: nextItemId++,
          videoId: m.videoId,
          title: clip(m.title, 120),
          artist: clip(m.artist, 80),
          by: ws.name,
        };
        room.queue.push(item);
        sendQueue(room);
        if (!item.title) enrich(item, room);
        break;
      }

      case 'queue_remove':
        room.queue = room.queue.filter((q) => q.id !== m.id);
        sendQueue(room);
        break;

      case 'queue_next': {
        // Ignore duplicate "next" requests for a song that has already moved on
        if (room.state && m.fromVideoId && room.state.videoId !== m.fromVideoId) return;
        const item = room.queue.shift();
        if (!item) return;
        room.state = { videoId: item.videoId, position: 0, playing: true, serverTs: Date.now(), by: ws.name };
        broadcast(room, { type: 'state', state: snapshot(room) }); // everyone, sender included
        sendQueue(room);
        break;
      }
    }
  });
  ws.on('close', () => leave(ws));
});

server.listen(PORT, () => console.log(`YT Music Jam on http://localhost:${PORT}`));
