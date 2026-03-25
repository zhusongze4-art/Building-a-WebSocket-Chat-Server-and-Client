package consumer;

import org.java_websocket.WebSocket;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RoomManager {

  // roomId -> set of connected WebSocket sessions
  private final ConcurrentHashMap<String, Set<WebSocket>> roomSessions = new ConcurrentHashMap<>();
  private final AtomicLong messagesProcessed = new AtomicLong(0);

  public void addSession(String roomId, WebSocket session) {
    roomSessions.computeIfAbsent(roomId, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
        .add(session);
  }

  public void removeSession(String roomId, WebSocket session) {
    Set<WebSocket> sessions = roomSessions.get(roomId);
    if (sessions != null) {
      sessions.remove(session);
    }
  }

  public void broadcast(String roomId, String jsonMessage) {
    Set<WebSocket> sessions = roomSessions.get(roomId);
    if (sessions == null || sessions.isEmpty()) return;

    for (WebSocket ws : sessions) {
      if (ws != null && ws.isOpen()) {
        try {
          ws.send(jsonMessage);
        } catch (Exception e) {
          System.err.println("[RoomManager] Failed to send to session: " + e.getMessage());
        }
      }
    }
    messagesProcessed.incrementAndGet();
  }

  public long getMessagesProcessed() {
    return messagesProcessed.get();
  }

  public int getSessionCount(String roomId) {
    Set<WebSocket> s = roomSessions.get(roomId);
    return s == null ? 0 : s.size();
  }
}