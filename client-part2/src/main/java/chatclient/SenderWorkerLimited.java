package chatclient;

import chatclient.metrics.Metrics;
import chatclient.model.ChatTask;
import chatclient.net.ConnectionPool;
import chatclient.net.PooledWebSocketClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class SenderWorkerLimited implements Runnable {

  private final BlockingQueue<ChatTask> queue;
  private final ConnectionPool pool;
  private final Metrics metrics;
  private final ObjectMapper mapper = new ObjectMapper();
  private final int quota;
  private final int maxRetries = 5;

  private final ConcurrentHashMap<Long, Long> pendingSendNanos;

  public SenderWorkerLimited(
      BlockingQueue<ChatTask> queue,
      ConnectionPool pool,
      Metrics metrics,
      int quota,
      ConcurrentHashMap<Long, Long> pendingSendNanos
  ) {
    this.queue = queue;
    this.pool = pool;
    this.metrics = metrics;
    this.quota = quota;
    this.pendingSendNanos = pendingSendNanos;
  }

  @Override
  public void run() {
    for (int i = 0; i < quota; i++) {
      ChatTask task;
      try {
        task = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      boolean ok = sendWithRetry(task);
      if (ok) metrics.success.increment();
      else metrics.failed.increment();
    }
  }

  private boolean sendWithRetry(ChatTask task) {
    long backoffMs = 20;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        PooledWebSocketClient client = pool.borrow(task.roomId);

        // record send timestamp before send
        pendingSendNanos.put(task.messageId, System.nanoTime());

        client.send(mapper.writeValueAsString(task.payload));
        return true;
      } catch (Exception e) {
        if (attempt == maxRetries) {
          pendingSendNanos.remove(task.messageId);
          return false;
        }
        try { Thread.sleep(backoffMs); } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          pendingSendNanos.remove(task.messageId);
          return false;
        }
        backoffMs *= 2;
      }
    }
    pendingSendNanos.remove(task.messageId);
    return false;
  }
}
