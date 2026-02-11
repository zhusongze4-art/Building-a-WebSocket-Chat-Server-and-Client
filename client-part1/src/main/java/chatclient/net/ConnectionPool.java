package chatclient.net;

import chatclient.metrics.Metrics;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {

  private final String host;
  private final int port;
  private final int connsPerRoom;
  private final Metrics metrics;

  private final ConcurrentHashMap<Integer, PooledWebSocketClient[]> pool = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, AtomicInteger> rr = new ConcurrentHashMap<>();

  /**
   * Part 1: basic load test connection pool (no latency stats, no CSV).
   */
  public ConnectionPool(String host, int port, int connsPerRoom, Metrics metrics) {
    this.host = host;
    this.port = port;
    this.connsPerRoom = connsPerRoom;
    this.metrics = metrics;
  }

  public PooledWebSocketClient borrow(int roomId) throws Exception {
    pool.computeIfAbsent(roomId, rid -> new PooledWebSocketClient[connsPerRoom]);
    rr.computeIfAbsent(roomId, rid -> new AtomicInteger(0));

    int idx = Math.floorMod(rr.get(roomId).getAndIncrement(), connsPerRoom);
    PooledWebSocketClient[] arr = pool.get(roomId);

    PooledWebSocketClient client = arr[idx];
    if (client == null || !client.isOpenSafe()) {
      URI uri = new URI("ws://" + host + ":" + port + "/chat/" + roomId);

      // Part 1: pass nulls so client does not record per-message metrics.
      PooledWebSocketClient nc = new PooledWebSocketClient(uri);

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
