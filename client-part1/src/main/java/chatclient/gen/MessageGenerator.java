package chatclient.gen;

import chatclient.model.ChatTask;
import chatclient.model.MessageType;
import chatclient.model.OutgoingMessage;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class MessageGenerator implements Runnable {

  private final BlockingQueue<ChatTask> queue;
  private final int totalMessages;
  private final Random rnd = new Random();
  private long nextId = 1;

  private static final List<String> MESSAGE_POOL = List.of(
      "Hello!", "How are you?", "Nice to meet you.", "Good morning!", "Good night!",
      "What's up?", "Let's go!", "Sounds good.", "Thanks!", "No problem.",
      "See you later.", "Great!", "Awesome!", "Interesting.", "LOL!",
      "I agree.", "I disagree.", "Maybe.", "Sure.", "Why not?",
      "Working on it.", "Almost done.", "Ping me later.", "Got it.", "Okay.",
      "This is fun.", "Let's chat.", "Any updates?", "Copy that.", "Roger.",
      "Nice!", "Cool.", "Wonderful.", "Amazing.", "Perfect.",
      "On my way.", "Busy now.", "Free now.", "Call me.", "Text me.",
      "Where are you?", "At home.", "At school.", "At work.", "In Seattle.",
      "Learning Java.", "Testing WebSocket.", "CS6650!", "Distributed systems.", "Bye!"
  );

  public MessageGenerator(BlockingQueue<ChatTask> queue, int totalMessages) {
    this.queue = queue;
    this.totalMessages = totalMessages;
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < totalMessages; i++) {
        ChatTask task = new ChatTask();
        task.roomId = 1 + rnd.nextInt(20);

        int uid = 1 + rnd.nextInt(100000);
        OutgoingMessage msg = new OutgoingMessage();
        msg.userId = String.valueOf(uid);
        msg.username = "user" + uid;

        long id = nextId++;
        task.messageId = id;

        // embed id in message so server echo lets us correlate ack
        String base = MESSAGE_POOL.get(rnd.nextInt(MESSAGE_POOL.size()));
        msg.message = base + "|id=" + id;

        msg.timestamp = OffsetDateTime.now().toString();

        int p = rnd.nextInt(100);
        if (p < 90) msg.messageType = MessageType.TEXT.name();
        else if (p < 95) msg.messageType = MessageType.JOIN.name();
        else msg.messageType = MessageType.LEAVE.name();

        task.payload = msg;
        task.attempt = 0;

        queue.put(task);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
