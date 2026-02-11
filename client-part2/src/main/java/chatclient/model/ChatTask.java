package chatclient.model;

public class ChatTask {
  public int roomId;                 // 1..20
  public long messageId;             // client-only id
  public OutgoingMessage payload;    // JSON body
  public int attempt;                // retry count
}
