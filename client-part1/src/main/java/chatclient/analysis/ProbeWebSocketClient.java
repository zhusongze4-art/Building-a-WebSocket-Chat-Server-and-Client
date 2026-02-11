package chatclient.analysis;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class ProbeWebSocketClient extends WebSocketClient {

  private final CompletableFuture<Void> openFuture = new CompletableFuture<>();
  private final AtomicReference<CompletableFuture<String>> nextMessageFuture = new AtomicReference<>();

  public ProbeWebSocketClient(URI serverUri) {
    super(serverUri);
  }

  @Override
  public void onOpen(ServerHandshake handshakedata) {
    openFuture.complete(null);
  }

  @Override
  public void onMessage(String message) {
    CompletableFuture<String> f = nextMessageFuture.getAndSet(null);
    if (f != null && !f.isDone()) {
      f.complete(message);
    }
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    openFuture.completeExceptionally(new RuntimeException("closed: " + reason));
    CompletableFuture<String> f = nextMessageFuture.getAndSet(null);
    if (f != null) f.completeExceptionally(new RuntimeException("closed"));
  }

  @Override
  public void onError(Exception ex) {
    openFuture.completeExceptionally(ex);
    CompletableFuture<String> f = nextMessageFuture.getAndSet(null);
    if (f != null) f.completeExceptionally(ex);
  }

  public CompletableFuture<Void> onOpenFuture() {
    return openFuture;
  }

  /** Prepare to await exactly ONE next message. Call this before send(). */
  public CompletableFuture<String> awaitNextMessage() {
    CompletableFuture<String> f = new CompletableFuture<>();
    nextMessageFuture.set(f);
    return f;
  }
}
