package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.sql.*;
import java.util.concurrent.Executors;

/**
 * HTTP server exposing metrics and analytics queries.
 * Uses HikariCP connection pool for fast query execution.
 */
public class MetricsServer {

  private final int port;
  private final DatabaseWriter dbWriter;
  private final RoomManager roomManager;
  private final ObjectMapper mapper = new ObjectMapper();
  private final HikariDataSource dataSource;

  public MetricsServer(int port, DatabaseWriter dbWriter, RoomManager roomManager) {
    this.port = port;
    this.dbWriter = dbWriter;
    this.roomManager = roomManager;

    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql://" + ConsumerMain.DB_HOST + ":3306/" + ConsumerMain.DB_NAME +
        "?useSSL=false&allowPublicKeyRetrieval=true");
    config.setUsername(ConsumerMain.DB_USER);
    config.setPassword(ConsumerMain.DB_PASS);
    config.setMaximumPoolSize(4);
    config.setConnectionTimeout(5000);
    this.dataSource = new HikariDataSource(config);
  }

  public void start() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.setExecutor(Executors.newFixedThreadPool(4));

    server.createContext("/metrics",          this::handleSystemMetrics);
    server.createContext("/metrics/rooms",    this::handleRoomStats);
    server.createContext("/metrics/users",    this::handleUserStats);
    server.createContext("/metrics/messages", this::handleRoomMessages);
    server.createContext("/metrics/all",      this::handleAllMetrics);

    server.start();
    System.out.println("[MetricsServer] Listening on port " + port);
  }

  // ── GET /metrics ──
  private void handleSystemMetrics(HttpExchange exchange) throws IOException {
    ObjectNode json = mapper.createObjectNode();
    json.put("totalBroadcast",   roomManager.getMessagesProcessed());
    json.put("totalDbWritten",   dbWriter.getTotalWritten());
    json.put("totalDbErrors",    dbWriter.getTotalErrors());
    json.put("totalBatches",     dbWriter.getTotalBatches());
    json.put("currentBuffer",    dbWriter.getBufferSize());
    json.put("dbPoolSize",       dbWriter.getPoolSize());
    sendJson(exchange, json.toString());
  }

  // ── GET /metrics/rooms ──
  private void handleRoomStats(HttpExchange exchange) throws IOException {
    String sql = "SELECT room_id, COUNT(*) as msg_count, " +
        "COUNT(DISTINCT user_id) as unique_users, " +
        "MIN(timestamp) as first_msg, MAX(timestamp) as last_msg " +
        "FROM messages GROUP BY room_id ORDER BY msg_count DESC";

    ArrayNode arr = mapper.createArrayNode();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setQueryTimeout(120);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ObjectNode row = mapper.createObjectNode();
          row.put("roomId",       rs.getString("room_id"));
          row.put("messageCount", rs.getLong("msg_count"));
          row.put("uniqueUsers",  rs.getInt("unique_users"));
          row.put("firstMessage", rs.getString("first_msg"));
          row.put("lastMessage",  rs.getString("last_msg"));
          arr.add(row);
        }
      }
    } catch (SQLException e) {
      sendError(exchange, e.getMessage());
      return;
    }
    sendJson(exchange, arr.toString());
  }

  // ── GET /metrics/users ──
  private void handleUserStats(HttpExchange exchange) throws IOException {
    String sql = "SELECT user_id, username, COUNT(*) as msg_count, " +
        "COUNT(DISTINCT room_id) as rooms_joined " +
        "FROM messages GROUP BY user_id, username " +
        "ORDER BY msg_count DESC LIMIT 20";

    ArrayNode arr = mapper.createArrayNode();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setQueryTimeout(120);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ObjectNode row = mapper.createObjectNode();
          row.put("userId",       rs.getString("user_id"));
          row.put("username",     rs.getString("username"));
          row.put("messageCount", rs.getLong("msg_count"));
          row.put("roomsJoined",  rs.getInt("rooms_joined"));
          arr.add(row);
        }
      }
    } catch (SQLException e) {
      sendError(exchange, e.getMessage());
      return;
    }
    sendJson(exchange, arr.toString());
  }

  // ── GET /metrics/messages?roomId=1&start=...&end=... ──
  private void handleRoomMessages(HttpExchange exchange) throws IOException {
    String query = exchange.getRequestURI().getQuery();
    String roomId = getParam(query, "roomId", "1");
    String start  = getParam(query, "start",  "2026-01-01T00:00:00");
    String end    = getParam(query, "end",    "2026-12-31T23:59:59");

    String sql = "SELECT message_id, user_id, username, message, message_type, timestamp " +
        "FROM messages WHERE room_id = ? AND timestamp BETWEEN ? AND ? " +
        "ORDER BY timestamp LIMIT 1000";

    ArrayNode arr = mapper.createArrayNode();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, roomId);
      ps.setString(2, start);
      ps.setString(3, end);
      ps.setQueryTimeout(60);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          ObjectNode row = mapper.createObjectNode();
          row.put("messageId",   rs.getString("message_id"));
          row.put("userId",      rs.getString("user_id"));
          row.put("username",    rs.getString("username"));
          row.put("message",     rs.getString("message"));
          row.put("messageType", rs.getString("message_type"));
          row.put("timestamp",   rs.getString("timestamp"));
          arr.add(row);
        }
      }
    } catch (SQLException e) {
      sendError(exchange, e.getMessage());
      return;
    }
    sendJson(exchange, arr.toString());
  }

  // ── GET /metrics/all ──
  private void handleAllMetrics(HttpExchange exchange) throws IOException {
    ObjectNode result = mapper.createObjectNode();

    // System stats
    ObjectNode system = mapper.createObjectNode();
    system.put("totalBroadcast", roomManager.getMessagesProcessed());
    system.put("totalDbWritten", dbWriter.getTotalWritten());
    system.put("totalDbErrors",  dbWriter.getTotalErrors());
    system.put("totalBatches",   dbWriter.getTotalBatches());
    result.set("system", system);

    try (Connection conn = dataSource.getConnection()) {

      // Total message count
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT COUNT(*) as total FROM messages")) {
        ps.setQueryTimeout(120);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) result.put("totalMessagesInDb", rs.getLong("total"));
        }
      }

      // Active users count (last hour)
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT COUNT(DISTINCT user_id) as cnt FROM messages " +
              "WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)")) {
        ps.setQueryTimeout(120);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) result.put("activeUsersLastHour", rs.getInt("cnt"));
        }
      }

      // Top 5 active rooms
      ArrayNode topRooms = mapper.createArrayNode();
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT room_id, COUNT(*) as cnt FROM messages " +
              "GROUP BY room_id ORDER BY cnt DESC LIMIT 5")) {
        ps.setQueryTimeout(120);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            ObjectNode r = mapper.createObjectNode();
            r.put("roomId", rs.getString("room_id"));
            r.put("messageCount", rs.getLong("cnt"));
            topRooms.add(r);
          }
        }
      }
      result.set("topRooms", topRooms);

      // Top 5 active users
      ArrayNode topUsers = mapper.createArrayNode();
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT user_id, username, COUNT(*) as cnt FROM messages " +
              "GROUP BY user_id, username ORDER BY cnt DESC LIMIT 5")) {
        ps.setQueryTimeout(120);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            ObjectNode u = mapper.createObjectNode();
            u.put("userId",   rs.getString("user_id"));
            u.put("username", rs.getString("username"));
            u.put("messageCount", rs.getLong("cnt"));
            topUsers.add(u);
          }
        }
      }
      result.set("topUsers", topUsers);

      // Messages per minute (last 10 minutes)
      ArrayNode throughput = mapper.createArrayNode();
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i') as minute_bucket, " +
              "COUNT(*) as cnt FROM messages " +
              "WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 10 MINUTE) " +
              "GROUP BY minute_bucket ORDER BY minute_bucket")) {
        ps.setQueryTimeout(120);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            ObjectNode t = mapper.createObjectNode();
            t.put("minute",       rs.getString("minute_bucket"));
            t.put("messageCount", rs.getLong("cnt"));
            throughput.add(t);
          }
        }
      }
      result.set("messagesPerMinute", throughput);

    } catch (SQLException e) {
      result.put("error", e.getMessage());
    }

    sendJson(exchange, result.toPrettyString());
  }

  // ── Helpers ──

  private void sendJson(HttpExchange exchange, String json) throws IOException {
    byte[] bytes = json.getBytes();
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private void sendError(HttpExchange exchange, String error) throws IOException {
    String json = "{\"error\": \"" + error.replace("\"", "'") + "\"}";
    byte[] bytes = json.getBytes();
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(500, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private String getParam(String query, String name, String defaultVal) {
    if (query == null) return defaultVal;
    for (String pair : query.split("&")) {
      String[] kv = pair.split("=", 2);
      if (kv.length == 2 && kv[0].equals(name)) return kv[1];
    }
    return defaultVal;
  }
}