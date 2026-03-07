package consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.List;

public class ConsumerWorker implements Runnable {

  private static final String EXCHANGE_NAME = "chat.exchange";

  private final Connection connection;
  private final RoomManager roomManager;
  private final List<String> rooms; // rooms this worker handles
  private final ObjectMapper mapper = new ObjectMapper();

  public ConsumerWorker(Connection connection, RoomManager roomManager, List<String> rooms) {
    this.connection = connection;
    this.roomManager = roomManager;
    this.rooms = rooms;
  }

  @Override
  public void run() {
    try {
      Channel channel = connection.createChannel();
      channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);

      // set prefetch - process up to 100 messages at a time
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
              roomManager.broadcast(msg.roomId, json);
              channel.basicAck(envelope.getDeliveryTag(), false);
            } catch (Exception e) {
              System.err.println("[ConsumerWorker] Error processing message: " + e.getMessage());
              // nack and requeue
              channel.basicNack(envelope.getDeliveryTag(), false, true);
            }
          }
        });
      }

      System.out.println("[ConsumerWorker] Listening on rooms: " + rooms);

      // keep thread alive
      while (!Thread.currentThread().isInterrupted()) {
        Thread.sleep(1000);
      }

    } catch (Exception e) {
      System.err.println("[ConsumerWorker] Fatal error: " + e.getMessage());
    }
  }
}