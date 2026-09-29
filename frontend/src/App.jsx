import { useCallback, useEffect, useState } from 'react';

function formatDate(value) {
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    month: 'short',
    day: 'numeric',
  }).format(new Date(value));
}

export default function App() {
  const [orders, setOrders] = useState([]);
  const [product, setProduct] = useState('Coffee');
  const [quantity, setQuantity] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const loadOrders = useCallback(async () => {
    try {
      const response = await fetch('/api/orders');
      if (!response.ok) throw new Error('The order API is not responding.');
      setOrders(await response.json());
    } catch (requestError) {
      setError(requestError.message);
    }
  }, []);

  useEffect(() => {
    loadOrders();
    const timer = window.setInterval(loadOrders, 2000);
    return () => window.clearInterval(timer);
  }, [loadOrders]);

  async function submitOrder(event) {
    event.preventDefault();
    setError('');
    setSubmitting(true);

    try {
      const response = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ product, quantity: Number(quantity) }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error ?? 'Could not submit the order.');
      setProduct('');
      setQuantity(1);
      await loadOrders();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setSubmitting(false);
    }
  }

  const processedCount = orders.filter((order) => order.status === 'processed').length;
  const pendingCount = orders.filter((order) => order.status === 'pending').length;

  return (
    <main className="shell">
      <header className="topbar">
        <a className="brand" href="#top" aria-label="OrderStream Lab home">
          <span className="brand-mark">O</span>
          <span>orderstream<span className="brand-light">.lab</span></span>
        </a>
        <div className="topbar-right"><span className="live-dot" /> Local learning environment</div>
      </header>

      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="eyebrow">KAFKA LEARNING PROJECT</p>
          <h1>Every order has<br /><span>a story to stream.</span></h1>
          <p className="hero-text">Create an order and follow it from the API into Kafka, through a worker, and into PostgreSQL.</p>
          <div className="flowline" aria-label="Order flow: browser to API to Kafka to worker to PostgreSQL">
            <span>Browser</span><i>→</i><span>API</span><i>→</i><span className="flow-active">Kafka</span><i>→</i><span>Worker</span><i>→</i><span>Postgres</span>
          </div>
        </div>
        <div className="hero-art" aria-hidden="true">
          <div className="orbit orbit-one" /><div className="orbit orbit-two" />
          <div className="core"><span>↗</span></div>
          <div className="orbit-node node-one">API</div><div className="orbit-node node-two">K</div><div className="orbit-node node-three">DB</div>
          <div className="signal signal-one" /><div className="signal signal-two" /><div className="signal signal-three" />
        </div>
      </section>

      <section className="dashboard-grid">
        <article className="panel create-panel">
          <div className="panel-heading">
            <div><p className="eyebrow">PRODUCER</p><h2>Create an order</h2></div>
            <span className="panel-icon">↗</span>
          </div>
          <form onSubmit={submitOrder}>
            <label htmlFor="product">Product</label>
            <input id="product" value={product} onChange={(event) => setProduct(event.target.value)} placeholder="e.g. Coffee" maxLength="100" required />
            <label htmlFor="quantity">Quantity</label>
            <input id="quantity" type="number" min="1" max="1000" value={quantity} onChange={(event) => setQuantity(event.target.value)} required />
            {error && <p className="error-message" role="alert">{error}</p>}
            <button className="submit-button" disabled={submitting} type="submit">{submitting ? 'Sending…' : 'Publish order event'} <span>→</span></button>
          </form>
          <p className="topic-note"><span className="topic-dot" /> Publishes to <code>orders.created</code></p>
        </article>

        <article className="panel stream-panel">
          <div className="panel-heading stream-heading">
            <div><p className="eyebrow">CONSUMER GROUP · ORDER-WORKERS</p><h2>Order stream</h2></div>
            <button className="refresh-button" onClick={loadOrders} type="button" aria-label="Refresh orders">↻</button>
          </div>
          <div className="stats-row">
            <div className="stat-card"><span className="stat-value">{orders.length}</span><span className="stat-label">Orders seen</span></div>
            <div className="stat-card"><span className="stat-value pending-color">{pendingCount}</span><span className="stat-label">Pending</span></div>
            <div className="stat-card"><span className="stat-value processed-color">{processedCount}</span><span className="stat-label">Processed</span></div>
          </div>
          <div className="orders-list">
            {orders.length === 0 ? (
              <div className="empty-state"><div className="empty-icon">⌁</div><strong>Your stream is quiet</strong><span>Publish an order to see the Kafka flow begin.</span></div>
            ) : orders.map((order) => (
              <div className="order-row" key={order.id}>
                <div className="order-symbol">{order.product.slice(0, 1).toUpperCase()}</div>
                <div className="order-details"><strong>{order.product} <span>× {order.quantity}</span></strong><small>{formatDate(order.created_at)} · {order.id.slice(0, 8)}</small></div>
                <span className={`status status-${order.status}`}>{order.status.replace('_', ' ')}</span>
              </div>
            ))}
          </div>
          <div className="stream-footer"><span className="live-dot" /> Refreshes every 2 seconds</div>
        </article>
      </section>

      <footer className="footer"><span>Built to learn event-driven systems</span><span>Kafka · Node.js · PostgreSQL</span></footer>
    </main>
  );
}
