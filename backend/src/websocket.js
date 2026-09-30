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
  const channelOperations = new Map();

  const forwardMessage = (channel, message) => {
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

  function serializeChannelOperation(channel, operation) {
    const previous = channelOperations.get(channel) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(operation);
    channelOperations.set(channel, next);
    const clear = () => {
      if (channelOperations.get(channel) === next) channelOperations.delete(channel);
    };
    void next.then(clear, clear);
    return next;
  }

  function attachChannel(webSocket, channel) {
    return serializeChannelOperation(channel, async () => {
      if (webSocket.readyState !== WebSocket.OPEN) return;
      let recipients = socketsByChannel.get(channel);
      if (!recipients) {
        recipients = new Set();
        socketsByChannel.set(channel, recipients);
        recipients.add(webSocket);
        channelsBySocket.set(webSocket, [...(channelsBySocket.get(webSocket) ?? []), channel]);
        try {
          await subscriber.subscribe(channel, (message) => forwardMessage(channel, message));
        } catch (error) {
          if (socketsByChannel.get(channel) === recipients) socketsByChannel.delete(channel);
          channelsBySocket.set(webSocket, (channelsBySocket.get(webSocket) ?? []).filter((item) => item !== channel));
          throw error;
        }
        return;
      }
      recipients.add(webSocket);
      const channels = channelsBySocket.get(webSocket) ?? [];
      channels.push(channel);
      channelsBySocket.set(webSocket, channels);
    });
  }

  function detachChannel(webSocket, channel) {
    return serializeChannelOperation(channel, async () => {
      const recipients = socketsByChannel.get(channel);
      if (!recipients) return;
      recipients.delete(webSocket);
      if (recipients.size) return;
      try {
        await subscriber.unsubscribe(channel);
      } catch (error) {
        logger.error('Could not unsubscribe Redis channel ' + channel + ':', error.message);
      } finally {
        if (socketsByChannel.get(channel) === recipients) socketsByChannel.delete(channel);
      }
    });
  }

  try {
    await subscriber.connect();
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
      if (!subscriber.isReady) {
        webSocket.send(JSON.stringify({ type: 'ready', channels, redisReady: false }));
        webSocket.send(JSON.stringify({ type: 'error', message: 'Live fan-out is waiting for Redis.' }));
        return;
      }
      Promise.all(channels.map((channel) => attachChannel(webSocket, channel)))
        .then(() => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.send(JSON.stringify({ type: 'ready', channels, redisReady: true }));
          }
        })
        .catch((error) => {
          logger.error('Could not subscribe WebSocket to sports updates:', error.message);
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.send(JSON.stringify({ type: 'error', message: 'Could not subscribe to live updates.' }));
            webSocket.close(1011, 'Redis subscription failed');
          }
        });
    });
  }

  webSockets.on('connection', (webSocket) => {
    webSocket.on('error', (error) => logger.error('Sports WebSocket error:', error.message));
    webSocket.on('close', () => {
      const channels = channelsBySocket.get(webSocket) ?? [];
      channelsBySocket.delete(webSocket);
      for (const channel of channels) void detachChannel(webSocket, channel);
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
