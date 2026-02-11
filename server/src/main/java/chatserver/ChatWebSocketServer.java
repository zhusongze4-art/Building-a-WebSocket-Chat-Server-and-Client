package chatserver;

import chatserver.model.ChatMessage;
import chatserver.model.ErrorResponse;
import chatserver.model.ServerResponse;
import chatserver.validation.MessageValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChatWebSocketServer extends WebSocketServer {

  private final ObjectMapper mapper;
  private final Map<WebSocket, String> connToRoom = new ConcurrentHashMap<>();

  public ChatWebSocketServer(InetSocketAddress address) {
    super(address);

    this.mapper = new ObjectMapper();
    // (optional) make Jackson handle Java time; but we're using strings anyway
    this.mapper.registerModule(new JavaTimeModule());
    this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public void onOpen(WebSocket conn, ClientHandshake handshake) {
    // Java-WebSocket gives path via handshake.getResourceDescriptor()
    String resource = handshake.getResourceDescriptor(); // e.g., "/chat/room123"
    String roomId = extractRoomId(resource);

    connToRoom.put(conn, roomId);

    System.out.println("[OPEN] " + conn.getRemoteSocketAddress() + " roomId=" + roomId);
  }

  @Override
  public void onMessage(WebSocket conn, String message) {
    String roomId = connToRoom.get(conn);
    try {
      ChatMessage msg = mapper.readValue(message, ChatMessage.class);

      // validate
      MessageValidator.validateOrThrow(
          roomId,
          msg.userId,
          msg.username,
          msg.message,
          msg.timestamp,
          msg.messageType
      );

      // echo success
      ServerResponse resp = new ServerResponse();
      resp.status = "OK";
      resp.serverTimestamp = OffsetDateTime.now().toString();
      resp.roomId = roomId;
      resp.echoed = msg;

      conn.send(mapper.writeValueAsString(resp));

    } catch (Exception e) {
      // error response
      try {
        ErrorResponse err = new ErrorResponse();
        err.status = "ERROR";
        err.serverTimestamp = OffsetDateTime.now().toString();
        err.roomId = roomId;
        err.error = e.getMessage();

        conn.send(mapper.writeValueAsString(err));
      } catch (Exception ignored) {
        // if JSON serialization fails, last resort
        conn.send("{\"status\":\"ERROR\",\"error\":\"internal error\"}");
      }
    }
  }

  @Override
  public void onClose(WebSocket conn, int code, String reason, boolean remote) {
    String roomId = connToRoom.remove(conn);
    System.out.println("[CLOSE] " + conn.getRemoteSocketAddress()
        + " roomId=" + roomId + " code=" + code + " reason=" + reason);
  }

  @Override
  public void onError(WebSocket conn, Exception ex) {
    System.out.println("[ERROR] " + (conn != null ? conn.getRemoteSocketAddress() : "server") +
        " " + ex.getMessage());
  }

  @Override
  public void onStart() {
    System.out.println("[START] WebSocket server started on " + getAddress());
  }

  private String extractRoomId(String resourceDescriptor) {
    // expected: /chat/{roomId}
    if (resourceDescriptor == null) return "";
    // normalize
    String path = resourceDescriptor.trim();
    // simple parse
    String prefix = "/chat/";
    if (!path.startsWith(prefix) || path.length() <= prefix.length()) {
      // path not matching, still accept but roomId empty
      return "";
    }
    String roomId = path.substring(prefix.length());
    // remove query string if any
    int q = roomId.indexOf('?');
    if (q >= 0) roomId = roomId.substring(0, q);
    return roomId;
  }
}
