package chatclient.analysis;

import java.net.URI;
import java.util.Arrays;

public class ConnOverheadMain {

  public static void main(String[] args) throws Exception {
    String host = "localhost";
    int port = 8080;
    int roomId = 1;

    int trials = 200;
    int warmup = 20;

    long[] times = new long[trials];

    for (int i = 0; i < trials; i++) {
      URI uri = new URI("ws://" + host + ":" + port + "/chat/" + roomId);

      ProbeWebSocketClient c = new ProbeWebSocketClient(uri);
      long t0 = System.nanoTime();
      c.connect();
      c.onOpenFuture().get();
      long t1 = System.nanoTime();

      c.close();
      times[i] = (t1 - t0) / 1_000_000; // ms
    }

    long[] data = Arrays.copyOfRange(times, warmup, times.length);
    Arrays.sort(data);

    long p50 = percentile(data, 50);
    long p95 = percentile(data, 95);
    long p99 = percentile(data, 99);
    double avg = Arrays.stream(data).average().orElse(0);

    System.out.println("=== Connection Overhead (ms) ===");
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
