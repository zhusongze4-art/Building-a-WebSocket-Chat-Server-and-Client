package chatclient.metrics3;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

public class LatencyStats {

  private final long[] latenciesMs;
  private int idx = 0;

  public final LongAdder okCount = new LongAdder();
  public final LongAdder errorCount = new LongAdder();

  public final LongAdder textCount = new LongAdder();
  public final LongAdder joinCount = new LongAdder();
  public final LongAdder leaveCount = new LongAdder();

  public final LongAdder[] roomCounts = new LongAdder[21]; // 1..20

  public LatencyStats(int expectedTotal) {
    this.latenciesMs = new long[expectedTotal];
    for (int i = 1; i <= 20; i++) roomCounts[i] = new LongAdder();
  }

  public synchronized void addLatency(long latencyMs) {
    if (idx < latenciesMs.length) {
      latenciesMs[idx++] = latencyMs;
    }
  }

  public void addRoom(int roomId) {
    if (roomId >= 1 && roomId <= 20) roomCounts[roomId].increment();
  }

  public void addType(String t) {
    if ("TEXT".equals(t)) textCount.increment();
    else if ("JOIN".equals(t)) joinCount.increment();
    else if ("LEAVE".equals(t)) leaveCount.increment();
  }

  public StatsResult summarize() {
    long[] data;
    int n;
    synchronized (this) {
      n = idx;
      data = Arrays.copyOf(latenciesMs, idx);
    }
    Arrays.sort(data);

    double mean = 0;
    for (long v : data) mean += v;
    mean = n == 0 ? 0 : mean / n;

    long min = n == 0 ? 0 : data[0];
    long max = n == 0 ? 0 : data[n - 1];
    long median = percentile(data, 50);
    long p95 = percentile(data, 95);
    long p99 = percentile(data, 99);

    return new StatsResult(n, mean, median, p95, p99, min, max);
  }

  private long percentile(long[] sorted, int p) {
    if (sorted.length == 0) return 0;
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    idx = Math.max(0, Math.min(idx, sorted.length - 1));
    return sorted[idx];
  }

  public record StatsResult(int n, double mean, long median, long p95, long p99, long min, long max) {}
}
