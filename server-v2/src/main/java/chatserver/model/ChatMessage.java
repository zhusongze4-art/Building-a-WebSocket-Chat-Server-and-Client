package chatserver.model;

public class ChatMessage {
  public String userId;       // spec says string, but must be 1..100000
  public String username;     // 3-20 alphanumeric
  public String message;      // 1-500 chars
  public String timestamp;    // ISO-8601 string
  public String messageType;  // "TEXT|JOIN|LEAVE"
}
