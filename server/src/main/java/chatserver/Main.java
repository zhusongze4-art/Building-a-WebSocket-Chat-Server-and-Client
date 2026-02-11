package chatserver;

import java.net.InetSocketAddress;

public class Main {

  public static void main(String[] args) throws Exception {
    int wsPort = 8080;      // WebSocket port
    int httpPort = 8081;    // Health port (can be same via full web framework, but we keep simple)

    ChatWebSocketServer wsServer = new ChatWebSocketServer(new InetSocketAddress(wsPort));
    wsServer.start();

    HealthServer healthServer = new HealthServer(httpPort);
    healthServer.start();

    System.out.println("WS: ws://localhost:" + wsPort + "/chat/{roomId}");
    System.out.println("Health: http://localhost:" + httpPort + "/health");
  }
}
