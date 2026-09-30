import { WebSocket, WebSocketServer } from 'ws';
import { closeRedisClient, createRedisClient } from './redis.js';

function writeHandshakeError(socket, status, message) {
  socket.write('HTTP/1.1 ' + status + '\r\nConnection: close\r\nContent-Length: ' + message.length +
    '\r\n\r\n' + message);
  socket.destroy();
}

function validKey(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{1,120}$/.test(value);
}

export async function attachSportsWebSocket(server, { logger = console } = {}) {
  const redisClient = createRedisClient();
  const subscriber = redisClient.duplicate();
  subscriber.on('error', (error) => logger.error('Redis subscriber error:', error.message));
  const webSockets = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
  const socketsByChannel = new Map();
  const channelsBySocket = new Map();
  let patternsReady = false;

  const forwardMessage = (message, channel) => {
    const recipients = socketsByChannel.get(channel);
    if (!recipients?.size) return;
    let envelope;
    try {
      envelope = JSON.parse(message);
    } catch {
      logger.error('Dropped unreadable sports Redis message from ' + channel + '.');
      return;
    }
    const payload = JSON.stringify({ type: 'update', ...envelope });
    for (const socket of recipients) {
      if (socket.readyState === WebSocket.OPEN) socket.send(payload);
    }
  };

  async function subscribePatterns() {
    if (patternsReady || !subscriber.isReady) return;
    await subscriber.pSubscribe('sports:match:*', (message, channel) => forwardMessage(message, channel));
    await subscriber.pSubscribe('sports:fan:*', (message, channel) => forwardMessage(message, channel));
    patternsReady = true;
  }

  try {
    await redisClient.connect();
    await subscriber.connect();
    await subscribePatterns();
  } catch (error) {
    logger.error('Sports WebSocket gateway could not connect to Redis:', error.message);
  }

  function upgrade(request, socket, head) {
    let url;
    try {
      url = new URL(request.url, 'http://localhost');
    } catch {
      writeHandshakeError(socket, '400 Bad Request', 'Bad request');
      return;
    }
    if (url.pathname !== '/ws') return;

    const matchId = url.searchParams.get('matchId');
    const fanId = url.searchParams.get('fanId');
    if ((!matchId && !fanId) || (matchId && !validKey(matchId)) || (fanId && !validKey(fanId))) {
      writeHandshakeError(socket, '400 Bad Request', 'Provide a valid matchId or fanId');
      return;
    }

    webSockets.handleUpgrade(request, socket, head, (webSocket) => {
      webSockets.emit('connection', webSocket, request);
      const channels = [
        ...(matchId ? ['sports:match:' + matchId] : []),
        ...(fanId ? ['sports:fan:' + fanId] : []),
      ];
      channelsBySocket.set(webSocket, channels);
      for (const channel of channels) {
        const recipients = socketsByChannel.get(channel) ?? new Set();
        recipients.add(webSocket);
        socketsByChannel.set(channel, recipients);
      }
      webSocket.send(JSON.stringify({
        type: 'ready',
        channels,
        redisReady: Boolean(subscriber.isReady && patternsReady),
      }));
      if (!subscriber.isReady || !patternsReady) {
        webSocket.send(JSON.stringify({ type: 'error', message: 'Live fan-out is waiting for Redis.' }));
      }
    });
  }

  webSockets.on('connection', (webSocket) => {
    webSocket.on('error', (error) => logger.error('Sports WebSocket error:', error.message));
    webSocket.on('close', () => {
      for (const channel of channelsBySocket.get(webSocket) ?? []) {
        const recipients = socketsByChannel.get(channel);
        recipients?.delete(webSocket);
        if (recipients?.size === 0) socketsByChannel.delete(channel);
      }
      channelsBySocket.delete(webSocket);
    });
  });

  server.on('upgrade', upgrade);

  return {
    async close() {
      server.off('upgrade', upgrade);
      for (const webSocket of webSockets.clients) webSocket.terminate();
      await new Promise((resolve) => webSockets.close(resolve));
      closeRedisClient(subscriber);
      closeRedisClient(redisClient);
    },
  };
}
