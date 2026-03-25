package consumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerMain {

  // RabbitMQ config
  private static final String RABBITMQ_HOST = "3.131.36.170";
  private static final String RABBITMQ_USER = "admin";
  private static final String RABBITMQ_PASS = "admin123";

  // MySQL config (same host as RabbitMQ) — package-visible for MetricsServer
  static final String DB_HOST     = "3.131.36.170";
  static final String DB_NAME     = "chatdb";
  static final String DB_USER     = "chatuser";
  static final String DB_PASS     = "chatpass123";

  // Threading config
  private static final int CONSUMER_THREADS = 20;
  private static final int TOTAL_ROOMS      = 20;
  private static final int WS_PORT          = 8080;
  private static final int METRICS_PORT     = 8081;  // NEW: Metrics API port

  public static void main(String[] args) throws Exception {
    // 1. Connect to RabbitMQ
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(RABBITMQ_HOST);
    factory.setUsername(RABBITMQ_USER);
    factory.setPassword(RABBITMQ_PASS);
    Connection connection = factory.newConnection();
    System.out.println("[ConsumerMain] Connected to RabbitMQ at " + RABBITMQ_HOST);

    // 2. Initialize database writer (NEW)
    DatabaseWriter dbWriter = new DatabaseWriter(DB_HOST, DB_NAME, DB_USER, DB_PASS);

    // 3. Shared room manager
    RoomManager roomManager = new RoomManager();

    // 4. Start WebSocket server for clients
    ConsumerWebSocketServer wsServer = new ConsumerWebSocketServer(
        new InetSocketAddress(WS_PORT), roomManager);
    wsServer.start();

    // 5. Start Metrics API server (NEW)
    MetricsServer metricsServer = new MetricsServer(METRICS_PORT, dbWriter, roomManager);
    metricsServer.start();

    // 6. Distribute rooms across consumer threads
    ExecutorService executor = Executors.newFixedThreadPool(CONSUMER_THREADS);
    int roomsPerThread = Math.max(1, TOTAL_ROOMS / CONSUMER_THREADS);

    for (int i = 0; i < CONSUMER_THREADS; i++) {
      int start = i * roomsPerThread + 1;
      int end = (i == CONSUMER_THREADS - 1) ? TOTAL_ROOMS : start + roomsPerThread - 1;

      List<String> rooms = new ArrayList<>();
      for (int r = start; r <= end; r++) {
        rooms.add(String.valueOf(r));
      }
      if (!rooms.isEmpty()) {
        executor.submit(new ConsumerWorker(connection, roomManager, dbWriter, rooms));
      }
    }

    System.out.println("[ConsumerMain] Started " + CONSUMER_THREADS + " consumer threads");
    System.out.println("[ConsumerMain] WebSocket on port " + WS_PORT);
    System.out.println("[ConsumerMain] Metrics API on port " + METRICS_PORT);

    // 7. Graceful shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("[ConsumerMain] Shutting down...");
      dbWriter.shutdown();
      executor.shutdown();
    }));

    // 8. Print stats every 10 seconds
    while (true) {
      Thread.sleep(10000);
      System.out.println("[Stats] Broadcast: " + roomManager.getMessagesProcessed() +
          " | DB Written: " + dbWriter.getTotalWritten() +
          " | DB Errors: " + dbWriter.getTotalErrors() +
          " | Buffer: " + dbWriter.getBufferSize());
    }
  }
}