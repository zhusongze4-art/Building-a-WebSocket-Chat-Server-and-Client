package chatclient.model;

public class OutgoingMessage {
  public String userId;
  public String username;
  public String message;
  public String timestamp;     // ISO-8601
  public String messageType;   // TEXT|JOIN|LEAVE
}
