package chatclient.net;

import chatclient.metrics3.CsvWriter;
import chatclient.metrics3.LatencyStats;
import chatclient.metrics3.ThroughputBuckets;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;

public class PooledWebSocketClient extends WebSocketClient {

  private final AtomicBoolean open = new AtomicBoolean(false);
  private final CountDownLatch openLatch = new CountDownLatch(1);

  private final ObjectMapper mapper = new ObjectMapper();

  // Optional (Part2/Part3 only). For Part1 they can be null.
  private final ConcurrentHashMap<Long, Long> pendingSendNanos;
  private final CsvWriter csv;
  private final LatencyStats stats;
  private final ThroughputBuckets buckets;

  public PooledWebSocketClient(
      URI serverUri,
      ConcurrentHashMap<Long, Long> pendingSendNanos,
      CsvWriter csv,
      LatencyStats stats,
      ThroughputBuckets buckets
  ) {
    super(serverUri);
    this.pendingSendNanos = pendingSendNanos;
    this.csv = csv;
    this.stats = stats;
    this.buckets = buckets;
  }

  @Override
  public void onOpen(ServerHandshake handshakedata) {
    open.set(true);
    openLatch.countDown();
  }

  @Override
  public void onMessage(String message) {
    // Part1: if you don't have pendingSendNanos, you cannot compute latency -> just ignore acks.
    if (pendingSendNanos == null) return;

    long ackMs = System.currentTimeMillis();

    try {
      JsonNode root = mapper.readTree(message);

      String status = root.has("status") ? root.get("status").asText() : "UNKNOWN";
      int roomId = root.has("roomId") ? root.get("roomId").asInt() : -1;

      String msgType = "UNKNOWN";
      String echoedMsg = null;

      if (root.has("echoed")) {
        JsonNode echoed = root.get("echoed");
        if (echoed.has("messageType")) msgType = echoed.get("messageType").asText();
        if (echoed.has("message")) echoedMsg = echoed.get("message").asText();
      }

      long id = parseIdFromMessage(echoedMsg);
      if (id <= 0) return;

      Long sendNanos = pendingSendNanos.remove(id);
      if (sendNanos == null) return;

      long latencyMs = Math.max(1, (System.nanoTime() - sendNanos) / 1_000_000);

      // stats (optional)
      if (stats != null) {
        stats.addLatency(latencyMs);
        stats.addType(msgType);
        stats.addRoom(roomId);

        if ("OK".equals(status)) stats.okCount.increment();
        else stats.errorCount.increment();
      }

      // throughput buckets (optional)
      if (buckets != null) {
        buckets.record(ackMs);
      }

      // CSV (optional)
      if (csv != null) {
        // timestamp,messageType,latency,statusCode,roomId
        String line = ackMs + "," + msgType + "," + latencyMs + "," + status + "," + roomId;
        csv.logLine(line);
      }

    } catch (Exception ignore) {
      // Never crash websocket callback thread
    }
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    open.set(false);
  }

  @Override
  public void onError(Exception ex) {
    open.set(false);
  }

  public boolean awaitOpen(long millis) throws InterruptedException {
    return openLatch.await(millis, TimeUnit.MILLISECONDS);
  }

  public boolean isOpenSafe() {
    return open.get() && isOpen();
  }

  private long parseIdFromMessage(String echoedMsg) {
    if (echoedMsg == null) return -1;
    int idx = echoedMsg.lastIndexOf("|id=");
    if (idx < 0) return -1;
    String idPart = echoedMsg.substring(idx + 4).trim();
    try {
      return Long.parseLong(idPart);
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
