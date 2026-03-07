package consumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerMain {


  private static final String RABBITMQ_HOST = "3.131.36.170";
  private static final String RABBITMQ_USER = "admin";
  private static final String RABBITMQ_PASS = "admin123";

  private static final int CONSUMER_THREADS = 20;  // number of consumer threads
  private static final int TOTAL_ROOMS = 20;
  private static final int WS_PORT = 8080;

  public static void main(String[] args) throws Exception {
    // connect to RabbitMQ
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(RABBITMQ_HOST);
    factory.setUsername(RABBITMQ_USER);
    factory.setPassword(RABBITMQ_PASS);
    Connection connection = factory.newConnection();
    System.out.println("[ConsumerMain] Connected to RabbitMQ at " + RABBITMQ_HOST);

    // shared room manager
    RoomManager roomManager = new RoomManager();

    // start WebSocket server for clients to connect to
    ConsumerWebSocketServer wsServer = new ConsumerWebSocketServer(
        new InetSocketAddress(WS_PORT), roomManager);
    wsServer.start();

    // distribute rooms across consumer threads
    // e.g. 20 threads, 20 rooms -> 1 room per thread
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
        executor.submit(new ConsumerWorker(connection, roomManager, rooms));
      }
    }

    System.out.println("[ConsumerMain] Started " + CONSUMER_THREADS + " consumer threads");
    System.out.println("[ConsumerMain] WebSocket listening on port " + WS_PORT);

    // print stats every 10 seconds
    while (true) {
      Thread.sleep(10000);
      System.out.println("[Stats] Messages processed: " + roomManager.getMessagesProcessed());
    }
  }
}