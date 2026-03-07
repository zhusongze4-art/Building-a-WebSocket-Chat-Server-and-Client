package chatserver;

import java.net.InetSocketAddress;

public class Main {


  private static final String RABBITMQ_HOST = "3.131.36.170";
  private static final String RABBITMQ_USER = "admin";
  private static final String RABBITMQ_PASS = "admin123";
  private static final int CHANNEL_POOL_SIZE = 20;

  public static void main(String[] args) throws Exception {
    int wsPort = 8080;
    int httpPort = 8081;

    System.out.println("[Main] Connecting to RabbitMQ at " + RABBITMQ_HOST);
    ChannelPool channelPool = new ChannelPool(
        RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, CHANNEL_POOL_SIZE);

    ChatWebSocketServer wsServer = new ChatWebSocketServer(
        new InetSocketAddress(wsPort), channelPool);
    wsServer.start();

    HealthServer healthServer = new HealthServer(httpPort);
    healthServer.start();

    System.out.println("WS:     ws://localhost:" + wsPort + "/chat/{roomId}");
    System.out.println("Health: http://localhost:" + httpPort + "/health");

    // graceful shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("[Main] Shutting down...");
      channelPool.shutdown();
    }));
  }
}