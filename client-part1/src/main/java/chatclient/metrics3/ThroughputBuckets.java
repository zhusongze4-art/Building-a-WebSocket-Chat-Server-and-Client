package chatclient.metrics3;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class ThroughputBuckets {

  private final long bucketMs;
  private final long startMs;
  private final ConcurrentHashMap<Long, LongAdder> buckets = new ConcurrentHashMap<>();

  public ThroughputBuckets(long startMs, long bucketMs) {
    this.startMs = startMs;
    this.bucketMs = bucketMs;
  }

  public void record(long eventTimeMs) {
    long bid = (eventTimeMs - startMs) / bucketMs;
    buckets.computeIfAbsent(bid, k -> new LongAdder()).increment();
  }

  public void printBucketsAsMsgPerSec() {
    // Sort buckets by id
    TreeMap<Long, Long> sorted = new TreeMap<>();
    for (Map.Entry<Long, LongAdder> e : buckets.entrySet()) {
      sorted.put(e.getKey(), e.getValue().sum());
    }

    System.out.println("=== Throughput over time (10s buckets) ===");
    System.out.println("bucketStartSec,msgPerSec");
    for (Map.Entry<Long, Long> e : sorted.entrySet()) {
      long bucketId = e.getKey();
      long count = e.getValue();
      long bucketStartSec = (bucketId * bucketMs) / 1000;
      double msgPerSec = count / (bucketMs / 1000.0);
      System.out.printf("%d,%.2f%n", bucketStartSec, msgPerSec);
    }
  }
}
