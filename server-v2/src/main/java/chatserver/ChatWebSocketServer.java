package chatserver;

import chatserver.model.ChatMessage;
import chatserver.model.ErrorResponse;
import chatserver.model.ServerResponse;
import chatserver.validation.MessageValidator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ChatWebSocketServer extends WebSocketServer {

  private final ObjectMapper mapper;
  private final Map<WebSocket, String> connToRoom = new ConcurrentHashMap<>();
  private final ChannelPool channelPool;
  private final String serverId;

  public ChatWebSocketServer(InetSocketAddress address, ChannelPool channelPool) {
    super(address);
    this.channelPool = channelPool;
    this.serverId = UUID.randomUUID().toString().substring(0, 8);

    this.mapper = new ObjectMapper();
    this.mapper.registerModule(new JavaTimeModule());
    this.mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public void onOpen(WebSocket conn, ClientHandshake handshake) {
    String resource = handshake.getResourceDescriptor();
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

      // build queue message
      QueueMessage qMsg = new QueueMessage();
      qMsg.messageId  = UUID.randomUUID().toString();
      qMsg.roomId     = roomId;
      qMsg.userId     = msg.userId;
      qMsg.username   = msg.username;
      qMsg.message    = msg.message;
      qMsg.timestamp  = msg.timestamp != null ? msg.timestamp : OffsetDateTime.now().toString();
      qMsg.messageType = msg.messageType;
      qMsg.serverId   = serverId;
      qMsg.clientIp   = conn.getRemoteSocketAddress().getAddress().getHostAddress();

      // publish to RabbitMQ
      String routingKey = "room." + roomId;
      byte[] body = mapper.writeValueAsBytes(qMsg);

      Channel ch = channelPool.borrowChannel();
      try {
        ch.basicPublish(ChannelPool.EXCHANGE_NAME, routingKey, null, body);
      } finally {
        channelPool.returnChannel(ch);
      }

      // send ACK back to sender
      ServerResponse resp = new ServerResponse();
      resp.status = "OK";
      resp.serverTimestamp = OffsetDateTime.now().toString();
      resp.roomId = roomId;
      resp.echoed = msg;
      conn.send(mapper.writeValueAsString(resp));

    } catch (Exception e) {
      try {
        ErrorResponse err = new ErrorResponse();
        err.status = "ERROR";
        err.serverTimestamp = OffsetDateTime.now().toString();
        err.roomId = roomId;
        err.error = e.getMessage();
        conn.send(mapper.writeValueAsString(err));
      } catch (Exception ignored) {
        conn.send("{\"status\":\"ERROR\",\"error\":\"internal error\"}");
      }
    }
  }

  @Override
  public void onClose(WebSocket conn, int code, String reason, boolean remote) {
    String roomId = connToRoom.remove(conn);
    System.out.println("[CLOSE] " + conn.getRemoteSocketAddress()
        + " roomId=" + roomId + " code=" + code);
  }

  @Override
  public void onError(WebSocket conn, Exception ex) {
    System.err.println("[ERROR] " + (conn != null ? conn.getRemoteSocketAddress() : "server")
        + " " + ex.getMessage());
  }

  @Override
  public void onStart() {
    System.out.println("[START] WebSocket server started on " + getAddress()
        + " serverId=" + serverId);
  }

  private String extractRoomId(String resourceDescriptor) {
    if (resourceDescriptor == null) return "";
    String path = resourceDescriptor.trim();
    String prefix = "/chat/";
    if (!path.startsWith(prefix) || path.length() <= prefix.length()) return "";
    String roomId = path.substring(prefix.length());
    int q = roomId.indexOf('?');
    if (q >= 0) roomId = roomId.substring(0, q);
    return roomId;
  }
}