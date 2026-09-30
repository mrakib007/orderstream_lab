CREATE TABLE IF NOT EXISTS sports_teams (
  team_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  short_name TEXT NOT NULL UNIQUE
);

INSERT INTO sports_teams (team_id, name, short_name)
VALUES
  ('northbridge', 'Northbridge FC', 'NBR'),
  ('rivergate', 'Rivergate United', 'RGU'),
  ('eastport', 'Eastport City', 'EPC'),
  ('highland', 'Highland Rovers', 'HLR')
ON CONFLICT (team_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS sports_matches (
  match_id TEXT PRIMARY KEY,
  competition TEXT NOT NULL,
  home_team_id TEXT NOT NULL REFERENCES sports_teams(team_id),
  away_team_id TEXT NOT NULL REFERENCES sports_teams(team_id),
  kickoff_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'scheduled'
    CHECK (status IN ('scheduled', 'live', 'half_time', 'finished')),
  home_score INTEGER NOT NULL DEFAULT 0 CHECK (home_score >= 0),
  away_score INTEGER NOT NULL DEFAULT 0 CHECK (away_score >= 0),
  last_event_sequence INTEGER NOT NULL DEFAULT 0 CHECK (last_event_sequence >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (home_team_id <> away_team_id)
);

INSERT INTO sports_matches (match_id, competition, home_team_id, away_team_id, kickoff_at)
VALUES
  ('match-northbridge-rivergate-01', 'OrderStream Learning League', 'northbridge', 'rivergate', NOW() + INTERVAL '1 day'),
  ('match-eastport-highland-01', 'OrderStream Learning League', 'eastport', 'highland', NOW() + INTERVAL '1 day')
ON CONFLICT (match_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS sports_matches_status_kickoff_idx
  ON sports_matches (status, kickoff_at);

ALTER TABLE sports_matches
  ADD COLUMN IF NOT EXISTS reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS sports_match_events (
  event_id UUID PRIMARY KEY,
  match_id TEXT NOT NULL REFERENCES sports_matches(match_id),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  event_type TEXT NOT NULL CHECK (
    event_type IN ('match_started', 'goal', 'yellow_card', 'red_card', 'half_time', 'match_completed')
  ),
  team_id TEXT REFERENCES sports_teams(team_id),
  player_name TEXT,
  match_minute SMALLINT CHECK (match_minute >= 0 AND match_minute <= 130),
  description TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ,
  kafka_topic TEXT,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (match_id, sequence)
);

CREATE INDEX IF NOT EXISTS sports_match_events_match_sequence_idx
  ON sports_match_events (match_id, sequence);
CREATE INDEX IF NOT EXISTS sports_match_events_occurred_at_idx
  ON sports_match_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS sports_match_events_created_at_idx
  ON sports_match_events (created_at DESC);

CREATE TABLE IF NOT EXISTS sports_simulation_jobs (
  match_id TEXT PRIMARY KEY REFERENCES sports_matches(match_id),
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  next_event_sequence INTEGER NOT NULL DEFAULT 1 CHECK (next_event_sequence > 0),
  last_error TEXT
);

ALTER TABLE sports_simulation_jobs
  ADD COLUMN IF NOT EXISTS lease_owner TEXT,
  ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS sports_fan_subscriptions (
  fan_id TEXT NOT NULL,
  team_id TEXT NOT NULL REFERENCES sports_teams(team_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (fan_id, team_id)
);

INSERT INTO sports_fan_subscriptions (fan_id, team_id)
VALUES ('demo-fan', 'northbridge'), ('demo-fan', 'eastport')
ON CONFLICT (fan_id, team_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS sports_fan_alerts (
  alert_id UUID PRIMARY KEY,
  fan_id TEXT NOT NULL,
  event_id UUID NOT NULL,
  match_id TEXT NOT NULL REFERENCES sports_matches(match_id),
  alert_type TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (fan_id, event_id, alert_type)
);

CREATE INDEX IF NOT EXISTS sports_fan_alerts_fan_created_idx
  ON sports_fan_alerts (fan_id, created_at DESC);

CREATE TABLE IF NOT EXISTS sports_outbox_events (
  outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id UUID NOT NULL,
  topic TEXT NOT NULL,
  message_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  locked_by TEXT,
  locked_until TIMESTAMPTZ,
  last_error TEXT,
  published_at TIMESTAMPTZ,
  kafka_partition INTEGER,
  kafka_offset BIGINT,
  UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS sports_outbox_pending_idx
  ON sports_outbox_events (next_attempt_at, outbox_id)
  WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS sports_analytics_minute (
  match_id TEXT NOT NULL REFERENCES sports_matches(match_id),
  bucket_start TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0),
  PRIMARY KEY (match_id, bucket_start, event_type)
);

CREATE TABLE IF NOT EXISTS sports_analytics_processed_events (
  event_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
