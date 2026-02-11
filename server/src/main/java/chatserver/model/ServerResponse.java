package chatserver.model;

public class ServerResponse {
  public String status;          // "OK"
  public String serverTimestamp; // ISO-8601
  public String roomId;

  public ChatMessage echoed;
}
