package chatclient.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PooledWebSocketClient extends WebSocketClient {

  private final AtomicBoolean open = new AtomicBoolean(false);
  private final CountDownLatch openLatch = new CountDownLatch(1);

  // Keep mapper in case you want to debug messages; not required for Part1.
  private final ObjectMapper mapper = new ObjectMapper();

  public PooledWebSocketClient(URI serverUri) {
    super(serverUri);
  }

  @Override
  public void onOpen(ServerHandshake handshakedata) {
    open.set(true);
    openLatch.countDown();
  }

  @Override
  public void onMessage(String message) {
    // Part1: basic load test -> do not record per-message latency, ignore acks.
  }

  @Override
  public void onClose(int code, String reason, boolean remote) {
    open.set(false);
  }

  @Override
  public void onError(Exception ex) {
    open.set(false);
  }

  public boolean awaitOpen(long millis) throws InterruptedException {
    return openLatch.await(millis, TimeUnit.MILLISECONDS);
  }

  public boolean isOpenSafe() {
    return open.get() && isOpen();
  }
}
