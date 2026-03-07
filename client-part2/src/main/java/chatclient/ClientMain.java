package chatclient;

import chatclient.gen.MessageGenerator;
import chatclient.metrics.Metrics;
import chatclient.metrics3.CsvWriter;
import chatclient.metrics3.LatencyStats;
import chatclient.metrics3.ThroughputBuckets;
import chatclient.net.ConnectionPool;

import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClientMain {

  public static void main(String[] args) throws Exception {
    String host = "chat-alb-230508812.us-east-2.elb.amazonaws.com";
    int wsPort = 80;

    int totalMessages = 500_000;

    int warmupThreads = 32;
    int warmupPerThread = 1000;
    int warmupMessages = warmupThreads * warmupPerThread;

    int connsPerRoom = 4;

    BlockingQueue<chatclient.model.ChatTask> queue = new ArrayBlockingQueue<>(200_000);

    // Part 3 shared components
    ConcurrentHashMap<Long, Long> pendingSendNanos = new ConcurrentHashMap<>();
    long startMs = System.currentTimeMillis();
    ThroughputBuckets buckets = new ThroughputBuckets(startMs, 1000); // 1s

    LatencyStats latencyStats = new LatencyStats(totalMessages);

    // CSV output file
    CsvWriter csv = new CsvWriter("metrics.csv");

    // 1) generator
    Thread gen = new Thread(new MessageGenerator(queue, totalMessages), "generator");
    gen.start();

    // 2) warmup
    Metrics warmupMetrics = new Metrics();
    ConnectionPool warmupPool = new ConnectionPool(
        host, wsPort, connsPerRoom, warmupMetrics,
        pendingSendNanos, csv, latencyStats, buckets
    );

    warmupMetrics.start();
    ExecutorService warmupExec = Executors.newFixedThreadPool(warmupThreads);
    CountDownLatch warmupLatch = new CountDownLatch(warmupThreads);

    for (int i = 0; i < warmupThreads; i++) {
      warmupExec.submit(() -> {
        try {
          new SenderWorkerLimited(queue, warmupPool, warmupMetrics, warmupPerThread, pendingSendNanos).run();
        } finally {
          warmupLatch.countDown();
        }
      });
    }

    warmupLatch.await();
    warmupExec.shutdownNow();
    warmupMetrics.end();

    System.out.println("=== WARMUP DONE ===");
    printPhaseMetrics(warmupMetrics);

    // 3) main phase
    Metrics mainMetrics = new Metrics();
    ConnectionPool mainPool = new ConnectionPool(
        host, wsPort, connsPerRoom, mainMetrics,
        pendingSendNanos, csv, latencyStats, buckets
    );

    int remaining = totalMessages - warmupMessages;
    int mainThreads = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);

    mainMetrics.start();
    ExecutorService mainExec = Executors.newFixedThreadPool(mainThreads);
    CountDownLatch mainLatch = new CountDownLatch(mainThreads);

    int perThread = remaining / mainThreads;
    int extra = remaining % mainThreads;

    for (int i = 0; i < mainThreads; i++) {
      int quota = perThread + (i < extra ? 1 : 0);
      mainExec.submit(() -> {
        try {
          new SenderWorkerLimited(queue, mainPool, mainMetrics, quota, pendingSendNanos).run();
        } finally {
          mainLatch.countDown();
        }
      });
    }

    mainLatch.await();
    mainExec.shutdownNow();
    mainMetrics.end();
    long waitUntil = System.currentTimeMillis() + 3000;
    while (!pendingSendNanos.isEmpty() && System.currentTimeMillis() < waitUntil) {
      Thread.sleep(50);
    }
    System.out.println("Pending acks left: " + pendingSendNanos.size());

    System.out.println("=== MAIN DONE ===");
    printPhaseMetrics(mainMetrics);

    // Ensure generator exits
    gen.join();

    // close CSV writer (flush)
    csv.close();

    // Part 3 statistical analysis
    System.out.println("=== PART 3: LATENCY STATS (ms) ===");
    var sr = latencyStats.summarize();
    System.out.println("Samples (acked): " + sr.n());
    System.out.printf("Mean:   %.2f%n", sr.mean());
    System.out.println("Median: " + sr.median());
    System.out.println("P95:    " + sr.p95());
    System.out.println("P99:    " + sr.p99());
    System.out.println("Min:    " + sr.min());
    System.out.println("Max:    " + sr.max());

    System.out.println("=== Message Type Distribution ===");
    System.out.println("TEXT:  " + latencyStats.textCount.sum());
    System.out.println("JOIN:  " + latencyStats.joinCount.sum());
    System.out.println("LEAVE: " + latencyStats.leaveCount.sum());

    System.out.println("=== Throughput per room (count only) ===");
    for (int r = 1; r <= 20; r++) {
      long c = latencyStats.roomCounts[r].sum();
      if (c > 0) System.out.println("room " + r + ": " + c);
    }

    // Throughput over time (10s buckets) - prints CSV-like lines
    buckets.printBucketsAsMsgPerSec();

    System.out.println("CSV written to: metrics.csv");
  }

  private static void printPhaseMetrics(Metrics m) {
    long ok = m.success.sum();
    long fail = m.failed.sum();
    double sec = m.wallSeconds();
    double tps = (ok + fail) / sec;

    System.out.println("Successful messages: " + ok);
    System.out.println("Failed messages:     " + fail);
    System.out.printf("Total runtime:       %.3f s%n", sec);
    System.out.printf("Throughput:          %.2f msg/s%n", tps);
    System.out.println("Total connections:   " + m.totalConnections.sum());
    System.out.println("Reconnections:       " + m.reconnections.sum());
  }
}
