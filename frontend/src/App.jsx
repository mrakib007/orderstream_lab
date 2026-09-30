import { useCallback, useEffect, useState } from 'react';
import SportsDashboard from './SportsDashboard.jsx';

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

function formatDate(value) {
  if (!value) return 'Not recorded yet';
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric', minute: '2-digit', month: 'short', day: 'numeric',
  }).format(new Date(value));
}
function shortId(value) { return value ? value.slice(0, 8) : '—'; }
function Status({ value }) {
  if (!value) return <span className="status status-unknown">waiting</span>;
  return <span className={'status status-' + value}>{value.replaceAll('_', ' ')}</span>;
}
function HealthPill({ label, service }) {
  const state = service?.ok ? 'online' : 'offline';
  return <span className={'health-pill health-' + state}><i /> {label} {state}</span>;
}
function TopicCard({ topic, link }) {
  return (
    <article className="topic-card">
      <div className="topic-card-head">
        <div><span className="micro-label">KAFKA TOPIC</span><h3>{topic.name}</h3></div>
        <a href={link} target="_blank" rel="noreferrer">Inspect messages ↗</a>
      </div>
      <p>{topic.partitionCount} partitions · one local broker · replication factor 1</p>
      <div className="partition-row" aria-label={topic.name + ' partitions'}>
        {topic.partitions.map((partition) => <span className="partition-chip" key={partition.id}>P{partition.id}</span>)}
      </div>
    </article>
  );
}
function GroupCard({ group, link }) {
  return (
    <article className="group-card">
      <div className="group-card-head">
        <div><span className="micro-label">CONSUMER GROUP</span><h3>{group.id}</h3></div>
        <span className={'group-state state-' + group.state}>{group.state}</span>
      </div>
      <div className="group-summary">
        <span><strong>{group.members.length}</strong> active members</span>
        <span><strong>{group.lagKnown ? group.lag : '—'}</strong> {group.lagKnown ? 'messages behind' : 'lag unknown'}</span>
        <a href={link} target="_blank" rel="noreferrer">Open in Kafbat ↗</a>
      </div>
      <div className="group-partitions">
        {group.partitions.map((partition) => (
          <span className="lag-chip" key={partition.partition}>
            P{partition.partition}: {partition.lag === null ? 'no committed offset' : partition.lag + ' lag'}
          </span>
        ))}
      </div>
      {group.members.length > 0 ? (
        <div className="member-list">
          {group.members.map((member) => (
            <div className="member-row" key={member.memberId}>
              <span className="member-dot" /><span>{(member.clientId || 'worker') + ' · ' + (member.memberId?.slice(-8) ?? '')}</span>
              <span className="assignment-text">
                {member.assignments.flatMap((assignment) => assignment.partitions.map((partition) => assignment.topic + ' P' + partition)).join(', ') || 'Assignment updating'}
              </span>
            </div>
          ))}
        </div>
      ) : (
        <p className="group-empty">{group.state === 'not-started' ? 'Start this worker to join the group and commit offsets.' : 'No active worker members right now.'}</p>
      )}
    </article>
  );
}

export default function App() {
  const [overview, setOverview] = useState(null);
  const [products, setProducts] = useState([]);
  const [orders, setOrders] = useState([]);
  const [selectedOrder, setSelectedOrder] = useState(null);
  const [productId, setProductId] = useState('coffee');
  const [quantity, setQuantity] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');

  const loadDashboard = useCallback(async (requestedOrderId) => {
    setRefreshing(true);
    setError('');
    try {
      const [nextOverview, nextProducts, nextOrders] = await Promise.all([
        getJson('/api/learning/overview'), getJson('/api/products'), getJson('/api/orders'),
      ]);
      const focusId = requestedOrderId || nextOrders[0]?.id;
      const detail = focusId ? await getJson('/api/orders/' + encodeURIComponent(focusId)) : null;
      setOverview(nextOverview);
      setProducts(nextProducts);
      setOrders(nextOrders);
      setSelectedOrder(detail);
      setProductId((current) => nextProducts.some((item) => item.product_id === current)
        ? current
        : (nextProducts[0]?.product_id ?? current));
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setRefreshing(false);
      setInitialLoading(false);
    }
  }, []);

  useEffect(() => { loadDashboard(null); }, [loadDashboard]);

  async function selectOrder(id) {
    setError('');
    try { setSelectedOrder(await getJson('/api/orders/' + encodeURIComponent(id))); }
    catch (requestError) { setError(requestError.message); }
  }

  async function submitOrder(event) {
    event.preventDefault();
    setError('');
    setNotice('');
    setSubmitting(true);
    try {
      const created = await getJson('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ productId, quantity: Number(quantity) }),
      });
      setNotice('One order was submitted. Refresh after the workers have had time to consume it.');
      setQuantity(1);
      await loadDashboard(created.id);
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setSubmitting(false);
    }
  }

  const totals = overview?.totals ?? {};
  const orderGroup = overview?.groups?.find((group) => group.id === 'order_processing_group');
  const notificationGroup = overview?.groups?.find((group) => group.id === 'notification_workers');
  const orderTopic = overview?.topics?.find((topic) => topic.name === 'order_events');
  const notificationTopic = overview?.topics?.find((topic) => topic.name === 'notification_events');
  const selectedProduct = products.find((item) => item.product_id === selectedOrder?.product_id);
  const trace = selectedOrder?.kafka ?? {
    topic: selectedOrder?.kafka_topic, key: selectedOrder?.kafka_key,
    partition: selectedOrder?.kafka_partition, offset: selectedOrder?.kafka_offset,
  };

  return (
    <main className="shell">
      <header className="topbar">
        <a className="brand" href="#top" aria-label="OrderStream Lab home">
          <span className="brand-mark">↗</span><span>OrderStream <span className="brand-light">Lab</span></span>
        </a>
        <div className="topbar-right">
          <HealthPill label="API" service={overview?.services?.api} />
          <HealthPill label="Kafka" service={overview?.services?.kafka} />
          <HealthPill label="Postgres" service={overview?.services?.database} />
          <a className="nav-link" href="http://localhost:8080" target="_blank" rel="noreferrer">Kafka UI ↗</a>
          <a className="nav-link" href="#sports">Sports Lab ↓</a>
          <a className="nav-link" href="#orderstream">OrderStream ↓</a>
        </div>
      </header>

      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="eyebrow">DISTRIBUTED EVENT-DRIVEN ARCHITECTURE</p>
          <h1>Follow one order<br /><span>through Kafka.</span></h1>
          <p className="hero-text">Submit one order, then trace its real message key, partition, consumer group, inventory result, and notification.</p>
          <div className="flowline" aria-label="Order flow">
            <span>Browser</span><i>→</i><span>Node API</span><i>→</i><span className="flow-active">Kafka</span><i>→</i><span>Workers</span><i>→</i><span>Postgres</span>
          </div>
        </div>
        <aside className="hero-note"><span className="note-icon">i</span><div><strong>Refresh is read-only</strong><p>This page never sends orders by itself. One form submission sends one order.</p></div></aside>
      </section>

      <SportsDashboard />

      <section className="metrics-grid" aria-label="Order totals" id="orderstream">
        <article className="metric-card"><span className="metric-icon">▤</span><div><span className="micro-label">TOTAL ORDERS</span><strong>{totals.total ?? '—'}</strong></div></article>
        <article className="metric-card"><span className="metric-icon metric-green">✓</span><div><span className="micro-label">CONFIRMED</span><strong className="text-green">{totals.confirmed ?? '—'}</strong></div></article>
        <article className="metric-card"><span className="metric-icon metric-amber">!</span><div><span className="micro-label">OUT OF STOCK</span><strong className="text-amber">{totals.out_of_stock ?? '—'}</strong></div></article>
        <article className="metric-card"><span className="metric-icon metric-blue">□</span><div><span className="micro-label">CATALOG PRODUCTS</span><strong className="text-blue">{products.length || '—'}</strong></div></article>
      </section>

      <section className="learning-panel">
        <div className="section-heading">
          <div><p className="eyebrow">INTERACTIVE KAFKA STEP-BY-STEP</p><h2>How Kafka processes this order</h2><p>Follow the four real steps below. Inspect the same topic and consumer group in Kafbat UI.</p></div>
          <button className="refresh-button" type="button" onClick={() => loadDashboard(selectedOrder?.id)} disabled={refreshing}>{refreshing ? 'Refreshing…' : '↻ Refresh data'}</button>
        </div>
        {error && <p className="error-message page-error" role="alert">{error}</p>}
        {notice && <p className="notice-message" role="status">{notice}</p>}
        {initialLoading && <p className="loading-message">Connecting to the local API…</p>}

        <div className="step-grid">
          <article className="step-card">
            <div className="step-top"><span className="step-number">1. START HERE</span><span className="step-role">Browser → API</span></div>
            <h3>Place one order</h3>
            <p className="step-description">The API saves a <b>pending</b> order in PostgreSQL, then publishes one event to Kafka.</p>
            <form className="order-form" onSubmit={submitOrder}>
              <label htmlFor="product">Catalog product</label>
              <select id="product" value={productId} onChange={(event) => setProductId(event.target.value)} required disabled={products.length === 0}>
                {products.map((product) => <option value={product.product_id} key={product.product_id}>{product.name} · {product.available_stock} in stock</option>)}
              </select>
              <label htmlFor="quantity">Quantity</label>
              <input id="quantity" type="number" min="1" max="1000" value={quantity} onChange={(event) => setQuantity(event.target.value)} required />
              <button className="submit-button" disabled={submitting || products.length === 0} type="submit">{submitting ? 'Submitting one order…' : 'Submit exactly one order'} <span>→</span></button>
            </form>
            <div className="step-callout"><b>What to notice</b><span>Pending means the API accepted the request. The order worker has not finished yet.</span></div>
          </article>

          <article className="step-card">
            <div className="step-top"><span className="step-number step-blue">2. INSPECT THIS</span><span className="step-role">Broker partition</span></div>
            <h3>Producer chooses a partition</h3>
            <p className="step-description">KafkaJS chooses a partition using the product key. The API saves the broker acknowledgement for this order.</p>
            <div className="trace-box">
              <div><span>Order ID</span><strong>{shortId(selectedOrder?.id)}</strong></div>
              <div><span>Topic</span><strong>{trace.topic || 'Waiting for an order'}</strong></div>
              <div><span>Message key</span><strong>{trace.key || '—'}</strong></div>
              <div className="trace-pair"><span>Partition <strong>{trace.partition === null || trace.partition === undefined ? '—' : 'P' + trace.partition}</strong></span><span>Offset <strong>{trace.offset ?? '—'}</strong></span></div>
            </div>
            <div className="step-callout callout-blue"><b>What to notice</b><span>Same product keys route consistently to one partition. Offsets count records within that partition.</span></div>
          </article>

          <article className="step-card">
            <div className="step-top"><span className="step-number step-amber">3. FOLLOW THE GROUP</span><span className="step-role">Consumer worker</span></div>
            <h3>Order group reads it</h3>
            <p className="step-description"><code>order_processing_group</code> shares the five order partitions among its active worker members.</p>
            {orderGroup ? <>
              <div className="group-inline"><span className={'group-state state-' + orderGroup.state}>{orderGroup.state}</span><span>{orderGroup.members.length} active member{orderGroup.members.length === 1 ? '' : 's'}</span><span>{orderGroup.lagKnown ? orderGroup.lag + ' total lag' : 'lag unknown'}</span></div>
              <div className="mini-partitions">{orderGroup.partitions.map((partition) => <span key={partition.partition}>P{partition.partition}<b>{partition.lag === null ? '—' : partition.lag}</b></span>)}</div>
            </> : <p className="group-empty">Kafka group details are not available yet.</p>}
            <a className="inspect-link" href="http://localhost:8080/ui/clusters/OrderStream%20local/consumer-groups/order_processing_group" target="_blank" rel="noreferrer">Open order group in Kafka UI ↗</a>
            <div className="step-callout callout-amber"><b>What to notice</b><span>Stop the worker, submit once, and refresh to observe pending work and lag. Start it again to drain the partition.</span></div>
          </article>

          <article className="step-card">
            <div className="step-top"><span className="step-number step-green">4. SEE THE RESULT</span><span className="step-role">Postgres + next topic</span></div>
            <h3>Inventory and notification</h3>
            <p className="step-description">The worker locks stock in PostgreSQL, saves the outcome, and publishes a notification event.</p>
            <div className="result-box">
              <div><span>Order state</span><Status value={selectedOrder?.status} /></div>
              <div><span>Stock remaining</span><strong>{selectedProduct ? selectedProduct.available_stock : 'Refresh to load'}</strong></div>
              <div><span>Notification</span><strong>{selectedOrder?.notification?.message ?? 'Waiting for notification worker'}</strong></div>
            </div>
            <div className="step-callout callout-green"><b>What to notice</b><span>Try quantity greater than available stock to see <code>out_of_stock</code>. No real email is sent.</span></div>
          </article>
        </div>
      </section>

      <section className="data-section">
        <div className="data-heading"><div><p className="eyebrow">REAL BROKER METADATA</p><h2>Topics and partitions</h2></div><span>Partition counts are read from Kafka</span></div>
        <div className="topic-grid">
          {orderTopic && <TopicCard topic={orderTopic} link="http://localhost:8080/ui/clusters/OrderStream%20local/topics/order_events/messages" />}
          {notificationTopic && <TopicCard topic={notificationTopic} link="http://localhost:8080/ui/clusters/OrderStream%20local/topics/notification_events/messages" />}
          {!orderTopic && <p className="group-empty">{overview?.errors?.kafka ? 'Kafka is unavailable: ' + overview.errors.kafka : 'Kafka topic metadata will appear after the API connects.'}</p>}
        </div>
      </section>

      <section className="data-section">
        <div className="data-heading"><div><p className="eyebrow">PARTITION OWNERSHIP AND PROGRESS</p><h2>Consumer groups</h2></div><span>Workers in the same group share partitions</span></div>
        <div className="group-grid">
          {orderGroup && <GroupCard group={orderGroup} link="http://localhost:8080/ui/clusters/OrderStream%20local/consumer-groups/order_processing_group" />}
          {notificationGroup && <GroupCard group={notificationGroup} link="http://localhost:8080/ui/clusters/OrderStream%20local/consumer-groups/notification_workers" />}
        </div>
      </section>

      <section className="data-section lower-grid">
        <article className="catalog-panel">
          <div className="data-heading"><div><p className="eyebrow">POSTGRESQL</p><h2>Product stock</h2></div><span>Updated from the database</span></div>
          <div className="stock-list">{products.map((product) => <div className="stock-row" key={product.product_id}><span>{product.name}</span><strong>{product.available_stock} available</strong></div>)}</div>
        </article>
        <article className="recent-panel">
          <div className="data-heading"><div><p className="eyebrow">POSTGRESQL</p><h2>Recent orders</h2></div><span>Select one to trace it</span></div>
          <div className="recent-list">
            {orders.length === 0 ? <p className="group-empty">No orders yet. The first one is yours to submit.</p> : orders.slice(0, 8).map((order) => (
              <button className={'recent-row' + (selectedOrder?.id === order.id ? ' recent-selected' : '')} key={order.id} type="button" onClick={() => selectOrder(order.id)}>
                <span className="recent-product">{order.product} × {order.quantity}</span>
                <span className="recent-meta">{formatDate(order.created_at)} · {shortId(order.id)}</span><Status value={order.status} />
              </button>
            ))}
          </div>
        </article>
      </section>
      <footer className="footer"><span>Kafka · Node.js · PostgreSQL · Redis · WebSockets · Podman</span><span>Sports metadata refreshes every 8 seconds · no automatic orders</span></footer>
    </main>
  );
}
