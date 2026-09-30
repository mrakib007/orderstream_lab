import { hostname } from 'node:os';
import { randomUUID } from 'node:crypto';
import { config } from '../config.js';
import { pool } from '../db.js';
import { createSportsSimulationModel } from '../models/sportsSimulationModel.js';

const simulationModel = createSportsSimulationModel(pool);
const workerId = hostname() + '-' + process.pid + '-' + randomUUID().slice(0, 8);
let stopping = false;

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function buildFixture(match) {
  return [
    { eventType: 'match_started', teamId: null, matchMinute: 0, description: 'Kick-off.' },
    { eventType: 'goal', teamId: match.homeTeamId, playerName: 'Amir Rahman', matchMinute: 18, description: match.homeTeam + ' score the opening goal.' },
    { eventType: 'yellow_card', teamId: match.awayTeamId, playerName: 'Leo Silva', matchMinute: 27, description: match.awayTeam + ' receive a yellow card.' },
    { eventType: 'goal', teamId: match.awayTeamId, playerName: 'Mika Tan', matchMinute: 41, description: match.awayTeam + ' equalize before the break.' },
    { eventType: 'half_time', teamId: null, matchMinute: 45, description: 'Half-time.' },
    { eventType: 'goal', teamId: match.homeTeamId, playerName: 'Noah Ellis', matchMinute: 66, description: match.homeTeam + ' retake the lead.' },
    { eventType: 'red_card', teamId: match.awayTeamId, playerName: 'Leo Silva', matchMinute: 78, description: match.awayTeam + ' are reduced to ten players.' },
    { eventType: 'match_completed', teamId: null, matchMinute: 90, description: 'Full-time.' },
  ];
}

async function simulate(job) {
  const match = await simulationModel.getMatchForSimulation(job.matchId);
  if (!match) throw new Error('Simulation job references an unknown match.');
  const fixture = buildFixture(match);

  for (let sequence = Number(job.nextEventSequence); sequence <= fixture.length; sequence += 1) {
    if (stopping) return;
    const item = fixture[sequence - 1];
    const event = {
      eventId: randomUUID(),
      matchId: match.matchId,
      sequence,
      eventType: item.eventType,
      teamId: item.teamId,
      playerName: item.playerName ?? null,
      matchMinute: item.matchMinute,
      description: item.description,
      occurredAt: new Date().toISOString(),
    };
    const result = await simulationModel.appendOutboxEvent({
      workerId,
      matchId: match.matchId,
      topic: config.sportsMatchEventsTopic,
      event,
    });
    if (result.kind === 'lease-lost') {
      console.log('Simulation lease was reclaimed: match=' + match.matchId);
      return;
    }
    if (result.kind !== 'appended' && result.kind !== 'completed') {
      throw new Error('Could not append event sequence ' + sequence + ': ' + result.kind + '.');
    }
    console.log('simulation match=' + match.matchId + ' sequence=' + sequence + ' event=' + item.eventType);
    if (result.kind === 'completed') return;
    await delay(Math.max(0, config.sportsSimulationEventDelayMs));
  }
  throw new Error('Simulation fixture ended before a completion event was stored.');
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  await pool.end().catch(() => {});
}

process.once('SIGINT', () => void shutdown());
process.once('SIGTERM', () => void shutdown());

while (!stopping) {
  try {
    const job = await simulationModel.claimNextJob(workerId);
    if (!job) {
      await delay(Math.max(100, config.sportsSimulatorPollDelayMs));
      continue;
    }
    try {
      await simulate(job);
    } catch (error) {
      await simulationModel.markJobFailed(job.matchId, workerId, error.message).catch((markError) => {
        console.error('Could not mark simulation job failed:', markError.message);
      });
      console.error('Simulation failed: match=' + job.matchId + ' error=' + error.message);
    }
  } catch (error) {
    console.error('Simulation worker database error:', error.message);
    await delay(Math.max(1000, config.sportsSimulatorPollDelayMs));
  }
}

await shutdown();
