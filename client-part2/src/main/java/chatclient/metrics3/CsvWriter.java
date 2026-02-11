package chatclient.metrics3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CsvWriter implements AutoCloseable {

  private final BlockingQueue<String> q = new LinkedBlockingQueue<>();
  private final Thread worker;
  private volatile boolean running = true;
  private final BufferedWriter bw;

  public CsvWriter(String path) throws IOException {
    bw = new BufferedWriter(new FileWriter(path));
    bw.write("timestamp,messageType,latency,statusCode,roomId");
    bw.newLine();
    bw.flush();

    worker = new Thread(() -> {
      try {
        while (running || !q.isEmpty()) {
          String line = q.poll();
          if (line == null) {
            try { Thread.sleep(5); } catch (InterruptedException ignored) {}
            continue;
          }
          bw.write(line);
          bw.newLine();
        }
      } catch (Exception ignored) {
      } finally {
        try { bw.flush(); bw.close(); } catch (IOException ignored) {}
      }
    }, "csv-writer");

    worker.start();
  }

  public void logLine(String csvLine) {
    q.offer(csvLine);
  }

  @Override
  public void close() {
    running = false;
    try { worker.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
  }
}
