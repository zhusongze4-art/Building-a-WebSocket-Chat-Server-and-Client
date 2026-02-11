package chatclient.net;

import chatclient.metrics.Metrics;
import chatclient.metrics3.CsvWriter;
import chatclient.metrics3.LatencyStats;
import chatclient.metrics3.ThroughputBuckets;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {

  private final String host;
  private final int port;
  private final int connsPerRoom;
  private final Metrics metrics;

  // Part 3 optional (can be null in client-part1)
  private final ConcurrentHashMap<Long, Long> pendingSendNanos;
  private final CsvWriter csv;
  private final LatencyStats stats;
  private final ThroughputBuckets buckets;

  private final ConcurrentHashMap<Integer, PooledWebSocketClient[]> pool = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, AtomicInteger> rr = new ConcurrentHashMap<>();

  /**
   * Part 1 constructor: no per-message latency metrics / CSV output.
   */
  public ConnectionPool(String host, int port, int connsPerRoom, Metrics metrics) {
    this(host, port, connsPerRoom, metrics, null, null, null, null);
  }

  /**
   * Part 2 / Part 3 constructor: enables per-message latency stats + CSV + throughput buckets.
   */
  public ConnectionPool(
      String host,
      int port,
      int connsPerRoom,
      Metrics metrics,
      ConcurrentHashMap<Long, Long> pendingSendNanos,
      CsvWriter csv,
      LatencyStats stats,
      ThroughputBuckets buckets
  ) {
    this.host = host;
    this.port = port;
    this.connsPerRoom = connsPerRoom;
    this.metrics = metrics;
    this.pendingSendNanos = pendingSendNanos;
    this.csv = csv;
    this.stats = stats;
    this.buckets = buckets;
  }

  public PooledWebSocketClient borrow(int roomId) throws Exception {
    pool.computeIfAbsent(roomId, rid -> new PooledWebSocketClient[connsPerRoom]);
    rr.computeIfAbsent(roomId, rid -> new AtomicInteger(0));

    int idx = Math.floorMod(rr.get(roomId).getAndIncrement(), connsPerRoom);
    PooledWebSocketClient[] arr = pool.get(roomId);

    PooledWebSocketClient client = arr[idx];
    if (client == null || !client.isOpenSafe()) {
      URI uri = new URI("ws://" + host + ":" + port + "/chat/" + roomId);

      // NOTE: pendingSendNanos/csv/stats/buckets may be null (client-part1)
      PooledWebSocketClient nc = new PooledWebSocketClient(uri, pendingSendNanos, csv, stats, buckets);

      nc.connect();
      boolean ok = nc.awaitOpen(3000);
      if (!ok) {
        try { nc.close(); } catch (Exception ignored) {}
        throw new IllegalStateException("Failed to connect to " + uri);
      }

      arr[idx] = nc;

      metrics.totalConnections.increment();
      if (client != null) metrics.reconnections.increment();
      client = nc;
    }
    return client;
  }
}
