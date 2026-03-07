package chatserver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class ChannelPool {

  public static final String EXCHANGE_NAME = "chat.exchange";

  private final BlockingQueue<Channel> pool;
  private Connection connection;

  public ChannelPool(String host, String user, String password, int poolSize)
      throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setUsername(user);
    factory.setPassword(password);
    factory.setPort(5672);

    this.connection = factory.newConnection();
    this.pool = new ArrayBlockingQueue<>(poolSize);

    // pre-create channels
    for (int i = 0; i < poolSize; i++) {
      Channel ch = connection.createChannel();
      // declare topic exchange (idempotent - safe to call multiple times)
      ch.exchangeDeclare(EXCHANGE_NAME, "topic", true);
      // declare one queue per room (room.1 through room.20)
      for (int r = 1; r <= 20; r++) {
        String queueName = "room." + r;
        ch.queueDeclare(queueName, true, false, false, null);
        ch.queueBind(queueName, EXCHANGE_NAME, "room." + r);
      }
      pool.offer(ch);
    }
    System.out.println("[ChannelPool] Initialized with " + poolSize + " channels");
  }

  public Channel borrowChannel() throws InterruptedException {
    return pool.take();
  }

  public void returnChannel(Channel ch) {
    if (ch != null && ch.isOpen()) {
      pool.offer(ch);
    }
  }

  public void shutdown() {
    try {
      for (Channel ch : pool) {
        if (ch.isOpen()) ch.close();
      }
      if (connection.isOpen()) connection.close();
    } catch (Exception e) {
      System.err.println("[ChannelPool] Error during shutdown: " + e.getMessage());
    }
  }
}