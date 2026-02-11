package chatserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class HealthServer {

  private final HttpServer server;

  public HealthServer(int port) throws Exception {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/health", this::handleHealth);
  }

  public void start() {
    server.start();
    System.out.println("[START] Health server started on " + server.getAddress());
  }

  private void handleHealth(HttpExchange exchange) {
    try {
      if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      String body = "{\"status\":\"ok\"}";
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, bytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(bytes);
      }
    } catch (Exception e) {
      try {
        exchange.sendResponseHeaders(500, -1);
      } catch (Exception ignored) {}
    }
  }
}
