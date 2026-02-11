package chatclient;

import chatclient.gen.MessageGenerator;
import chatclient.metrics.Metrics;
import chatclient.net.ConnectionPool;

import java.util.concurrent.*;

public class ClientMain {

  public static void main(String[] args) throws Exception {
    String host = "localhost";
    int wsPort = 8080;

    int totalMessages = 500_000;

    int warmupThreads = 32;
    int warmupPerThread = 1000;
    int warmupMessages = warmupThreads * warmupPerThread;

    int connsPerRoom = 4;

    BlockingQueue<chatclient.model.ChatTask> queue = new ArrayBlockingQueue<>(200_000);

    // 1) generator
    Thread gen = new Thread(new MessageGenerator(queue, totalMessages), "generator");
    gen.start();

    // 2) warmup phase (Part 1 only)
    Metrics warmupMetrics = new Metrics();
    ConnectionPool warmupPool = new ConnectionPool(host, wsPort, connsPerRoom, warmupMetrics);

    warmupMetrics.start();
    ExecutorService warmupExec = Executors.newFixedThreadPool(warmupThreads);
    CountDownLatch warmupLatch = new CountDownLatch(warmupThreads);

    for (int i = 0; i < warmupThreads; i++) {
      warmupExec.submit(() -> {
        try {
          new SenderWorkerLimited(queue, warmupPool, warmupMetrics, warmupPerThread, null).run();
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

    // 3) main phase (Part 1 only)
    Metrics mainMetrics = new Metrics();
    ConnectionPool mainPool   = new ConnectionPool(host, wsPort, connsPerRoom, mainMetrics);

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
          new SenderWorkerLimited(queue, mainPool, mainMetrics, quota, null).run();
        } finally {
          mainLatch.countDown();
        }
      });
    }

    mainLatch.await();
    mainExec.shutdownNow();
    mainMetrics.end();

    System.out.println("=== MAIN DONE ===");
    printPhaseMetrics(mainMetrics);

    // Ensure generator exits
    gen.join();
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
