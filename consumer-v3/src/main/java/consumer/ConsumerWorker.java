package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.List;

public class ConsumerWorker implements Runnable {

  private static final String EXCHANGE_NAME = "chat.exchange";

  private final Connection connection;
  private final RoomManager roomManager;
  private final DatabaseWriter dbWriter;       // NEW
  private final List<String> rooms;
  private final ObjectMapper mapper = new ObjectMapper();

  public ConsumerWorker(Connection connection, RoomManager roomManager,
      DatabaseWriter dbWriter, List<String> rooms) {
    this.connection = connection;
    this.roomManager = roomManager;
    this.dbWriter = dbWriter;                // NEW
    this.rooms = rooms;
  }

  @Override
  public void run() {
    try {
      Channel channel = connection.createChannel();
      channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
      channel.basicQos(100);

      for (String roomId : rooms) {
        String queueName = "room." + roomId;
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, "room." + roomId);

        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(String consumerTag, Envelope envelope,
              AMQP.BasicProperties properties, byte[] body) throws IOException {
            try {
              QueueMessage msg = mapper.readValue(body, QueueMessage.class);
              String json = mapper.writeValueAsString(msg);

              // 1. Broadcast to connected WebSocket clients (same as before)
              roomManager.broadcast(msg.roomId, json);

              // 2. Send to database writer buffer (NEW)
              dbWriter.addMessage(msg);

              channel.basicAck(envelope.getDeliveryTag(), false);
            } catch (Exception e) {
              System.err.println("[ConsumerWorker] Error processing message: " + e.getMessage());
              channel.basicNack(envelope.getDeliveryTag(), false, true);
            }
          }
        });
      }

      System.out.println("[ConsumerWorker] Listening on rooms: " + rooms);

      while (!Thread.currentThread().isInterrupted()) {
        Thread.sleep(1000);
      }

    } catch (Exception e) {
      System.err.println("[ConsumerWorker] Fatal error: " + e.getMessage());
    }
  }
}