import { useCallback, useEffect, useMemo, useState } from 'react';

async function getJson(url, options) {
  const response = await fetch(url, options);
  const body = await response.text();
  let result = {};
  if (body) {
    try { result = JSON.parse(body); }
    catch { throw new Error('The API returned an unreadable response.'); }
  }
  if (!response.ok) throw new Error(result.error ?? 'Request failed with HTTP ' + response.status + '.');
  return result;
}

function formatTime(value) {
  if (!value) return '—';
  return new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' }).format(new Date(value));
}

function formatLatency(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value < 1000 ? Math.max(0, Math.round(value)) + ' ms' : (value / 1000).toFixed(2) + ' s';
}

function MatchScore({ match }) {
  return (
    <div className="sports-scoreboard">
      <div className="score-comp"><span>{match.competition}</span><span>{new Date(match.kickoffAt).toLocaleDateString()}</span></div>
      <div className="score-teams">
        <div className="score-team"><span className="team-badge">{match.homeShortName}</span><strong>{match.homeTeam}</strong></div>
        <div className="score-value"><b>{match.homeScore}</b><i>:</i><b>{match.awayScore}</b></div>
        <div className="score-team score-away"><span className="team-badge">{match.awayShortName}</span><strong>{match.awayTeam}</strong></div>
      </div>
      <div className="score-footer">
        <span className={'sports-status status-' + match.status}>{match.status.replaceAll('_', ' ')}</span>
        <span>Sequence {match.lastEventSequence}</span>
        {match.reconciliationRequired && <strong className="reconcile-tag">reconciliation required</strong>}
      </div>
    </div>
  );
}

function EventRow({ event }) {
  return (
    <article className="sports-event-row">
      <div className="event-minute">{event.matchMinute === null || event.matchMinute === undefined ? '—' : event.matchMinute + "'"}</div>
      <div className="event-main">
        <div className="event-title"><strong>{event.eventType?.replaceAll('_', ' ')}</strong><span>#{event.sequence}</span></div>
        <p>{event.description}</p>
        <div className="event-trace">
          <span>key <b>{event.key ?? event.matchId}</b></span>
          <span>{event.kafkaTopic ?? event.topic ?? 'Kafka pending'}</span>
          <span>P{event.kafkaPartition ?? event.partition ?? '—'}</span>
          <span>offset {event.kafkaOffset ?? event.offset ?? '—'}</span>
          <time>{formatTime(event.occurredAt)}</time>
        </div>
      </div>
    </article>
  );
}

function TopicTelemetry({ topics = [] }) {
  return (
    <div className="sports-topic-grid">
      {topics.map((topic) => (
        <article className="sports-topic" key={topic.name}>
          <div className="sports-topic-head"><strong>{topic.name}</strong><span>{topic.partitionCount} partitions</span></div>
          <div className="sports-topic-parts">
            {topic.partitions.map((partition) => (
              <span className="sports-partition" key={partition.id} title={'Partition ' + partition.id + ', leader ' + partition.leader}>
                P{partition.id}<small>L{partition.leader}</small>
              </span>
            ))}
          </div>
          <a href={'http://localhost:8080/ui/clusters/OrderStream%20local/topics/' + encodeURIComponent(topic.name) + '/messages'} target="_blank" rel="noreferrer">Inspect in Kafka UI ↗</a>
        </article>
      ))}
    </div>
  );
}

function ConsumerGroupTelemetry({ groups = [] }) {
  return (
    <div className="sports-groups-grid">
      {groups.map((group) => (
        <article className="sports-group" key={group.id}>
          <div className="sports-group-head"><strong>{group.id}</strong><span className={'group-state state-' + group.state}>{group.state}</span></div>
          <div className="sports-group-meta"><span>{group.members.length} active member{group.members.length === 1 ? '' : 's'}</span><b>{group.lagKnown ? group.lag + ' total lag' : 'lag unknown'}</b></div>
          <div className="sports-lag-list">
            {group.partitions.map((partition) => (
              <span className="lag-chip" key={(partition.topic ?? group.topic) + ':' + partition.partition}>
                {(partition.topic ?? group.topic).split('.').at(-1)} P{partition.partition}: {partition.lag === null ? 'no offset' : partition.lag + ' lag'}
              </span>
            ))}
          </div>
          {group.members.length ? group.members.map((member) => (
            <div className="sports-member" key={member.memberId}>
              <span>{member.clientId || 'worker'} · {member.memberId.slice(-8)}</span>
              <small>{member.assignments.flatMap((assignment) => assignment.partitions.map((partition) => assignment.topic + ' P' + partition)).join(', ') || 'Assignment updating'}</small>
            </div>
          )) : <p className="sports-empty">Start this worker in a separate terminal to join the group.</p>}
        </article>
      ))}
    </div>
  );
}

export default function SportsDashboard() {
  const [matches, setMatches] = useState([]);
  const [selectedMatchId, setSelectedMatchId] = useState('');
  const [events, setEvents] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [telemetry, setTelemetry] = useState(null);
  const [connection, setConnection] = useState('connecting');
  const [latencies, setLatencies] = useState([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const selectedMatch = matches.find((match) => match.matchId === selectedMatchId);
  const updateEvents = useCallback((incoming) => {
    setEvents((current) => {
      const byId = new Map(current.map((item) => [item.eventId, item]));
      for (const item of incoming) byId.set(item.eventId, { ...byId.get(item.eventId), ...item });
      return [...byId.values()].sort((a, b) => a.sequence - b.sequence).slice(-40);
    });
  }, []);

  const loadEvents = useCallback(async (matchId) => {
    if (!matchId) return;
    const result = await getJson('/api/sports/matches/' + encodeURIComponent(matchId) + '/events');
    if (matchId === selectedMatchId) updateEvents(result);
  }, [selectedMatchId, updateEvents]);

  const refreshTelemetry = useCallback(async () => {
    const [nextTelemetry, nextMatches, nextAlerts] = await Promise.all([
      getJson('/api/sports/telemetry'),
      getJson('/api/sports/matches'),
      getJson('/api/sports/fan-alerts?fanId=demo-fan'),
    ]);
    setTelemetry(nextTelemetry);
    setMatches(nextMatches);
    setAlerts(nextAlerts);
    setSelectedMatchId((current) => current && nextMatches.some((match) => match.matchId === current)
      ? current
      : (nextMatches[0]?.matchId ?? ''));
  }, []);

  useEffect(() => {
    let active = true;
    const refresh = async () => {
      try {
        const [nextTelemetry, nextMatches, nextAlerts] = await Promise.all([
          getJson('/api/sports/telemetry'),
          getJson('/api/sports/matches'),
          getJson('/api/sports/fan-alerts?fanId=demo-fan'),
        ]);
        if (!active) return;
        setError('');
        setTelemetry(nextTelemetry);
        setMatches(nextMatches);
        setAlerts(nextAlerts);
        setSelectedMatchId((current) => current && nextMatches.some((match) => match.matchId === current)
          ? current
          : (nextMatches[0]?.matchId ?? ''));
      } catch (requestError) {
        if (active) setError(requestError.message);
      }
    };
    void refresh();
    const interval = setInterval(() => void refresh(), 8000);
    return () => { active = false; clearInterval(interval); };
  }, []);

  useEffect(() => {
    if (!selectedMatchId) return undefined;
    let active = true;
    setEvents([]);
    void getJson('/api/sports/matches/' + encodeURIComponent(selectedMatchId) + '/events')
      .then((result) => { if (active) updateEvents(result); })
      .catch((requestError) => { if (active) setError(requestError.message); });

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socketUrl = protocol + '//' + window.location.host + '/ws?matchId=' +
      encodeURIComponent(selectedMatchId) + '&fanId=demo-fan';
    const socket = new WebSocket(socketUrl);
    setConnection('connecting');
    socket.onmessage = (message) => {
      try {
        const payload = JSON.parse(message.data);
        if (payload.type === 'ready') {
          setConnection(payload.redisReady ? 'live' : 'waiting for Redis');
          if (payload.redisReady) {
            void loadEvents(selectedMatchId).catch((requestError) => setError(requestError.message));
            void getJson('/api/sports/fan-alerts?fanId=demo-fan').then(setAlerts).catch(() => {});
          }
          return;
        }
        if (payload.type === 'error') {
          setConnection('waiting for Redis');
          return;
        }
        if (payload.type !== 'update' || !payload.event) return;
        const { event, source = {} } = payload;
        if (event.matchId === selectedMatchId) {
          const observed = Date.now() - Date.parse(event.occurredAt);
          if (Number.isFinite(observed)) setLatencies((current) => [...current, Math.max(0, observed)].slice(-20));
          updateEvents([{
            ...event,
            kafkaTopic: source.topic,
            kafkaPartition: source.partition,
            kafkaOffset: source.offset,
            key: source.key,
          }]);
        }
        if (event.fanId === 'demo-fan') {
          setAlerts((current) => [
            {
              alertId: event.eventId,
              eventId: event.sourceEventId,
              matchId: event.matchId,
              alertType: event.alertType,
              message: event.message,
              createdAt: event.createdAt,
            },
            ...current.filter((alert) => alert.alertId !== event.eventId && alert.eventId !== event.sourceEventId),
          ].slice(0, 50));
        }
      } catch {
        setError('Received an unreadable WebSocket update.');
      }
    };
    socket.onopen = () => setConnection('connected');
    socket.onerror = () => { if (active) setConnection('offline'); };
    socket.onclose = () => { if (active) setConnection('offline'); };
    return () => {
      active = false;
      socket.close();
    };
  }, [selectedMatchId, loadEvents, updateEvents]);

  const simulate = async () => {
    if (!selectedMatchId) return;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const result = await getJson('/api/sports/matches/' + encodeURIComponent(selectedMatchId) + '/simulate', {
        method: 'POST',
      });
      setNotice(result.resumed
        ? 'The failed simulation was queued to resume from its next durable event.'
        : 'Simulation queued. The sports simulator worker will publish eight ordered fixture events.');
      await refreshTelemetry();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusy(false);
    }
  };

  const averageSocketLatency = useMemo(() => latencies.length
    ? latencies.reduce((sum, value) => sum + value, 0) / latencies.length
    : null, [latencies]);
  const metrics = telemetry?.metrics ?? {};
  const canSimulate = selectedMatch && (
    selectedMatch.simulationStatus === 'failed' ||
    (selectedMatch.status === 'scheduled' && !['queued', 'running', 'completed'].includes(selectedMatch.simulationStatus))
  );

  return (
    <section className="sports-lab" id="sports">
      <div className="sports-heading">
        <div><p className="eyebrow">LIVE SPORTS EVENT LAB</p><h2>Match center</h2><p>Kafka orders match events by <code>matchId</code>. Node sends Redis updates to this browser; it never connects to Kafka.</p></div>
        <div className={'socket-badge socket-' + connection.replaceAll(' ', '-') }><i />{connection}</div>
      </div>
      {error && <p className="error-message page-error" role="alert">{error}</p>}
      {notice && <p className="notice-message" role="status">{notice}</p>}

      <div className="sports-control-row">
        <label htmlFor="sports-match">Match</label>
        <select id="sports-match" value={selectedMatchId} onChange={(event) => setSelectedMatchId(event.target.value)}>
          {matches.map((match) => <option value={match.matchId} key={match.matchId}>{match.homeShortName} vs {match.awayShortName} · {match.competition}</option>)}
        </select>
        <button className="sports-run-button" type="button" onClick={simulate} disabled={!canSimulate || busy}>
          {busy ? 'Queueing…' : selectedMatch?.simulationStatus === 'failed' ? 'Resume simulation' : 'Simulate one match'} <span>→</span>
        </button>
        {selectedMatch?.simulationStatus && <span className="simulation-state">job {selectedMatch.simulationStatus}</span>}
      </div>

      {selectedMatch ? <MatchScore match={selectedMatch} /> : <p className="sports-empty">Sports matches appear after the additive database setup has been initialized.</p>}

      <div className="sports-metrics-grid">
        <article><span>PROJECTED EVENTS / MIN</span><strong>{metrics.eventsLastMinute ?? '—'}</strong></article>
        <article><span>PROJECTED EVENTS / SEC</span><strong>{Number.isFinite(metrics.eventsPerSecond) ? metrics.eventsPerSecond.toFixed(2) : '—'}</strong></article>
        <article><span>AVG PROJECTION LATENCY</span><strong>{formatLatency(metrics.averageProjectionLatencyMs)}</strong></article>
        <article><span>AVG BROWSER UPDATE LATENCY</span><strong>{formatLatency(averageSocketLatency)}</strong></article>
      </div>

      <div className="sports-content-grid">
        <article className="sports-panel">
          <div className="sports-panel-heading"><div><span className="micro-label">ORDERED MATCH EVENT LOG</span><h3>Live timeline</h3></div><span>{events.length} visible</span></div>
          <div className="sports-events">
            {events.length ? events.slice().reverse().map((event) => <EventRow event={event} key={event.eventId} />)
              : <p className="sports-empty">No projected events yet. Queue a simulation and start the workers shown below.</p>}
          </div>
        </article>
        <article className="sports-panel">
          <div className="sports-panel-heading"><div><span className="micro-label">REDIS FAN CHANNEL</span><h3>Demo fan alerts</h3></div><span>{alerts.length} stored</span></div>
          <div className="sports-alerts">
            {alerts.length ? alerts.slice(0, 10).map((alert) => (
              <div className="sports-alert" key={alert.alertId}>
                <span className="alert-icon">✦</span><div><strong>{alert.alertType?.replaceAll('_', ' ')}</strong><p>{alert.message}</p><time>{formatTime(alert.createdAt)}</time></div>
              </div>
            )) : <p className="sports-empty">The demo fan follows Northbridge FC and Eastport City.</p>}
          </div>
        </article>
      </div>

      <div className="sports-data-section">
        <div className="sports-section-title"><div><span className="micro-label">KAFKA PARTITIONS</span><h3>Topics and keys</h3></div><span>Six match partitions · three alert partitions · one DLQ partition</span></div>
        {telemetry?.errors?.kafka && <p className="sports-empty">Kafka metadata: {telemetry.errors.kafka}</p>}
        <TopicTelemetry topics={telemetry?.topics} />
      </div>

      <div className="sports-data-section">
        <div className="sports-section-title"><div><span className="micro-label">INDEPENDENT GROUP OFFSETS</span><h3>Consumers and lag</h3></div><span>Groups have independent replay positions</span></div>
        <ConsumerGroupTelemetry groups={telemetry?.groups} />
      </div>

      <details className="sports-commands">
        <summary>Open six worker roles in separate terminals</summary>
        <p>The simulate button only queues one match. Run these roles to publish, project, fan out, alert, and count events.</p>
        <div>{['npm run sports:simulator', 'npm run sports:outbox', 'npm run sports:score', 'npm run sports:fanout', 'npm run sports:alerts', 'npm run sports:analytics'].map((command) => <code key={command}>{command}</code>)}</div>
      </details>

      <details className="sports-commands">
        <summary>Reliability lessons: retry, dedupe, replay, and dead letters</summary>
        <p>The simulator and fan-alert worker save events in PostgreSQL outbox rows before Kafka publication. The publisher retries with per-key ordering; a crash after Kafka accepts a record can send it again.</p>
        <p>Consumers deduplicate by event ID, and the score projection checks the next match sequence before changing the score. A gap pauses that match for reconciliation instead of guessing.</p>
        <p>Each group keeps its own Kafka offsets. Replaying a group repeats delivery, so idempotent database writes protect scores, alerts, and analytics. After bounded retries, the failed record and source coordinates go to the dead-letter topic.</p>
        <p>Redis Pub/Sub is live fan-out and can lose updates while a browser is disconnected. The dashboard reloads the durable event history from PostgreSQL when the socket connects.</p>
      </details>
    </section>
  );
}
