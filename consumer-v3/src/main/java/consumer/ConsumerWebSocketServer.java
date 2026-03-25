package consumer;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;

public class ConsumerWebSocketServer extends WebSocketServer {

  private final RoomManager roomManager;

  public ConsumerWebSocketServer(InetSocketAddress address, RoomManager roomManager) {
    super(address);
    this.roomManager = roomManager;
  }

  @Override
  public void onOpen(WebSocket conn, ClientHandshake handshake) {
    String roomId = extractRoomId(handshake.getResourceDescriptor());
    roomManager.addSession(roomId, conn);
    conn.setAttachment(roomId);
    System.out.println("[ConsumerWS] Client joined room=" + roomId
        + " from " + conn.getRemoteSocketAddress());
  }

  @Override
  public void onClose(WebSocket conn, int code, String reason, boolean remote) {
    String roomId = conn.getAttachment();
    if (roomId != null) {
      roomManager.removeSession(roomId, conn);
    }
    System.out.println("[ConsumerWS] Client left room=" + roomId);
  }

  @Override
  public void onMessage(WebSocket conn, String message) {
    // Consumer WS server only broadcasts - it doesn't receive messages from clients
  }

  @Override
  public void onError(WebSocket conn, Exception ex) {
    System.err.println("[ConsumerWS] Error: " + ex.getMessage());
  }

  @Override
  public void onStart() {
    System.out.println("[ConsumerWS] WebSocket server started on " + getAddress());
  }

  private String extractRoomId(String resourceDescriptor) {
    if (resourceDescriptor == null) return "";
    String prefix = "/chat/";
    if (!resourceDescriptor.startsWith(prefix)) return "";
    String roomId = resourceDescriptor.substring(prefix.length());
    int q = roomId.indexOf('?');
    if (q >= 0) roomId = roomId.substring(0, q);
    return roomId;
  }
}