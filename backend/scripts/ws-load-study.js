import { performance } from 'node:perf_hooks';
import { WebSocket } from 'ws';

const args = new Map();
for (let index = 2; index < process.argv.length; index += 1) {
  const key = process.argv[index];
  if (key.startsWith('--')) {
    const value = process.argv[index + 1];
    if (value && !value.startsWith('--')) {
      args.set(key, value);
      index += 1;
    } else {
      args.set(key, true);
    }
  }
}

function numberArg(name, fallback, minimum, maximum) {
  const value = args.has(name) ? Number(args.get(name)) : fallback;
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(name + ' must be a whole number from ' + minimum + ' to ' + maximum + '.');
  }
  return value;
}

function percentile(values, ratio) {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * ratio) - 1)];
}

function ms(value) {
  return value === null ? '—' : Math.round(value) + ' ms';
}

const usage = 'Usage: node backend/scripts/ws-load-study.js --connections 10 --i-understand-local-load ' +
  '[--step 10] [--step-seconds 30] [--ramp-delay-ms 100] [--url ws://127.0.0.1:3000/ws?matchId=match-northbridge-rivergate-01&fanId=demo-fan]';

if (!args.has('--i-understand-local-load') || !args.has('--connections')) {
  console.error('This tool opens local WebSocket connections only; it does not send orders or create events.');
  console.error(usage);
  process.exitCode = 2;
} else {
  try {
    const maximum = numberArg('--connections', 0, 1, 1000);
    const step = numberArg('--step', 10, 1, 100);
    const stepSeconds = numberArg('--step-seconds', 30, 5, 300);
    const rampDelayMs = numberArg('--ramp-delay-ms', 100, 0, 3000);
    const url = new URL(args.get('--url') === true || !args.has('--url')
      ? 'ws://127.0.0.1:3000/ws?matchId=match-northbridge-rivergate-01&fanId=demo-fan'
      : args.get('--url'));
    if (!['ws:', 'wss:'].includes(url.protocol) || !['localhost', '127.0.0.1', '[::1]', '::1'].includes(url.hostname)) {
      throw new Error('The study tool accepts localhost WebSocket URLs only.');
    }
    if (url.pathname !== '/ws') throw new Error('The study URL must use the local /ws endpoint.');

    const active = new Set();
    const sockets = new Set();
    const handshakeTimes = [];
    const updateLatencies = [];
    const counters = { attempts: 0, connected: 0, failed: 0, closed: 0, updates: 0, bytes: 0 };
    let stopping = false;

    function closeAll() {
      stopping = true;
      for (const socket of sockets) {
        if (socket.readyState === WebSocket.OPEN) socket.close(1000, 'Local study rung complete');
        else if (socket.readyState === WebSocket.CONNECTING) socket.terminate();
      }
    }
    process.once('SIGINT', closeAll);
    process.once('SIGTERM', closeAll);

    function connectOne() {
      counters.attempts += 1;
      const startedAt = performance.now();
      return new Promise((resolve) => {
        const socket = new WebSocket(url);
        sockets.add(socket);
        let settled = false;
        const timeout = setTimeout(() => {
          if (settled) return;
          settled = true;
          counters.failed += 1;
          socket.terminate();
          resolve(false);
        }, 10000);

        socket.on('open', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          counters.connected += 1;
          active.add(socket);
          handshakeTimes.push(performance.now() - startedAt);
          resolve(true);
        });
        socket.on('message', (data) => {
          counters.bytes += data.length;
          let message;
          try { message = JSON.parse(data.toString()); }
          catch { return; }
          if (message.type !== 'update' || !message.event?.occurredAt) return;
          const latency = Date.now() - Date.parse(message.event.occurredAt);
          if (Number.isFinite(latency)) {
            counters.updates += 1;
            updateLatencies.push(Math.max(0, latency));
          }
        });
        socket.on('error', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          counters.failed += 1;
          resolve(false);
        });
        socket.on('close', () => {
          active.delete(socket);
          counters.closed += 1;
          if (!settled) {
            settled = true;
            clearTimeout(timeout);
            counters.failed += 1;
            resolve(false);
          }
        });
      });
    }

    const sleep = (duration) => new Promise((resolve) => setTimeout(resolve, duration));
    console.log('Local WebSocket study: ' + maximum + ' maximum sockets, ' + step + ' per rung, ' + stepSeconds + ' seconds per rung.');
    console.log('The script sends no HTTP requests and creates no match events. Queue at most one match manually if you want event-latency samples.');

    for (let target = Math.min(step, maximum); target <= maximum && !stopping; target = Math.min(maximum, target + step)) {
      const before = { ...counters };
      const newConnections = target - active.size;
      for (let index = 0; index < newConnections && !stopping; index += 1) {
        await connectOne();
        if (rampDelayMs) await sleep(rampDelayMs);
      }
      console.log('Rung ' + target + '/' + maximum + ' active. Observe Podman/Windows resources; queue at most one fixture from the browser if desired.');
      await sleep(stepSeconds * 1000);
      const rungUpdates = counters.updates - before.updates;
      const rungSeconds = stepSeconds;
      const processRssMiB = Math.round(process.memoryUsage().rss / (1024 * 1024));
      console.log(JSON.stringify({
        targetConnections: target,
        activeConnections: active.size,
        newConnectionFailures: counters.failed - before.failed,
        handshakeP50Ms: ms(percentile(handshakeTimes.slice(-newConnections), 0.5)),
        handshakeP95Ms: ms(percentile(handshakeTimes.slice(-newConnections), 0.95)),
        updateDeliveries: rungUpdates,
        updateDeliveriesPerSecond: Number((rungUpdates / rungSeconds).toFixed(2)),
        updateLatencyP50Ms: ms(percentile(updateLatencies.slice(before.updates), 0.5)),
        updateLatencyP95Ms: ms(percentile(updateLatencies.slice(before.updates), 0.95)),
        receivedBytes: counters.bytes - before.bytes,
        processRssMiB,
      }));
      if (target === maximum) break;
    }
    closeAll();
    await sleep(250);
    console.log(JSON.stringify({
      attempts: counters.attempts,
      opened: counters.connected,
      failed: counters.failed,
      closed: counters.closed,
      remainingOpen: active.size,
      totalUpdateDeliveries: counters.updates,
    }));
  } catch (error) {
    console.error(error.message);
    console.error(usage);
    process.exitCode = 2;
  }
}
