package chatclient;

import chatclient.metrics.Metrics;
import chatclient.model.ChatTask;
import chatclient.net.ConnectionPool;
import chatclient.net.PooledWebSocketClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SenderWorker implements Runnable {

  private final BlockingQueue<ChatTask> queue;
  private final ConnectionPool pool;
  private final Metrics metrics;
  private final ObjectMapper mapper = new ObjectMapper();
  private final int maxRetries = 5;

  public SenderWorker(BlockingQueue<ChatTask> queue, ConnectionPool pool, Metrics metrics) {
    this.queue = queue;
    this.pool = pool;
    this.metrics = metrics;
  }

  @Override
  public void run() {
    while (true) {
      ChatTask task;
      try {
        task = queue.poll(2, TimeUnit.SECONDS);
        if (task == null) return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      boolean sent = sendWithRetry(task);
      if (sent) metrics.success.increment();
      else metrics.failed.increment();
    }
  }

  private boolean sendWithRetry(ChatTask task) {
    long backoffMs = 20;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        PooledWebSocketClient client = pool.borrow(task.roomId);
        String json = mapper.writeValueAsString(task.payload);
        client.send(json);
        return true;
      } catch (Exception e) {
        if (attempt == maxRetries) return false;
        try { Thread.sleep(backoffMs); } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
        backoffMs *= 2;
      }
    }
    return false;
  }
}
