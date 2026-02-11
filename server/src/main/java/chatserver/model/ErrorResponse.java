package chatserver.model;

public class ErrorResponse {
  public String status;          // "ERROR"
  public String serverTimestamp; // ISO-8601
  public String roomId;

  public String error;           // human readable error message
}
