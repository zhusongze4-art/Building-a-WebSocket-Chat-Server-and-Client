package chatclient.analysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import chatclient.model.OutgoingMessage;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;

public class RttProbeMain {

  public static void main(String[] args) throws Exception {
    String host = "localhost";
    int port = 8080;
    int roomId = 1;

    int trials = 100;
    int warmup = 10;

    ObjectMapper mapper = new ObjectMapper();

    URI uri = new URI("ws://" + host + ":" + port + "/chat/" + roomId);
    ProbeWebSocketClient client = new ProbeWebSocketClient(uri);

    client.connect();
    client.onOpenFuture().get();

    long[] rtts = new long[trials];

    for (int i = 0; i < trials; i++) {
      OutgoingMessage msg = new OutgoingMessage();
      msg.userId = "1";
      msg.username = "user1";
      msg.message = "probe";
      msg.timestamp = OffsetDateTime.now().toString();
      msg.messageType = "TEXT";

      String json = mapper.writeValueAsString(msg);

      var fut = client.awaitNextMessage();
      long t0 = System.nanoTime();
      client.send(json);
      String echo = fut.get();
      long t1 = System.nanoTime();


      JsonNode node = mapper.readTree(echo);
      if (node.has("status") && !"OK".equals(node.get("status").asText())) {
        System.out.println("Got non-OK response: " + echo);
      }

      rtts[i] = (t1 - t0) / 1_000_000; // ms
    }

    client.close();


    long[] data = Arrays.copyOfRange(rtts, warmup, rtts.length);
    Arrays.sort(data);

    long p50 = percentile(data, 50);
    long p95 = percentile(data, 95);
    long p99 = percentile(data, 99);
    double avg = Arrays.stream(data).average().orElse(0);

    System.out.println("=== RTT Probe (ms) ===");
    System.out.println("Trials (used): " + data.length);
    System.out.printf("Avg: %.2f ms%n", avg);
    System.out.println("P50: " + p50 + " ms");
    System.out.println("P95: " + p95 + " ms");
    System.out.println("P99: " + p99 + " ms");
  }

  private static long percentile(long[] sorted, int p) {
    if (sorted.length == 0) return 0;
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    idx = Math.max(0, Math.min(idx, sorted.length - 1));
    return sorted[idx];
  }
}
