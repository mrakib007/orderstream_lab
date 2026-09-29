CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY,
  product TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  status TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'out_of_stock', 'publish_failed', 'processed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS products (
  product_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  available_stock INTEGER NOT NULL CHECK (available_stock >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO products (product_id, name, available_stock)
VALUES
  ('coffee', 'Coffee', 40),
  ('keyboard', 'Keyboard', 10),
  ('mouse', 'Mouse', 30),
  ('monitor', 'Monitor', 2),
  ('headphones', 'Headphones', 0)
ON CONFLICT (product_id) DO NOTHING;

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS product_id TEXT REFERENCES products(product_id),
  ADD COLUMN IF NOT EXISTS event_id UUID,
  ADD COLUMN IF NOT EXISTS notification_event_id UUID,
  ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
  ADD COLUMN IF NOT EXISTS kafka_key TEXT,
  ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
  ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'orders'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%status%'
  LOOP
    EXECUTE format('ALTER TABLE orders DROP CONSTRAINT %I', constraint_name);
  END LOOP;
END $$;

ALTER TABLE orders
  ADD CONSTRAINT orders_status_check
  CHECK (status IN ('pending', 'confirmed', 'out_of_stock', 'publish_failed', 'processed'));

CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS orders_status_idx ON orders (status);

CREATE TABLE IF NOT EXISTS notifications (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id UUID NOT NULL UNIQUE REFERENCES orders(id),
  event_id UUID NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('confirmed', 'out_of_stock', 'processed')),
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS notifications_created_at_idx ON notifications (created_at DESC);
