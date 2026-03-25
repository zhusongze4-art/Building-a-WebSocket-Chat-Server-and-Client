package consumer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batched, async database writer.
 * Messages are added to a buffer and flushed either when the batch is full
 * or when a timer fires — whichever comes first.
 */
public class DatabaseWriter {

  // ── Tuning knobs (test different values for Part 2) ──
  private static final int    BATCH_SIZE     = 500;   // flush every N messages
  private static final long   FLUSH_INTERVAL = 500;   // flush every N ms
  private static final int    POOL_SIZE      = 10;    // DB connection pool size
  private static final int    WRITER_THREADS = 4;     // parallel writer threads

  // ── SQL ──
  private static final String INSERT_SQL =
      "INSERT IGNORE INTO messages " +
          "(message_id, room_id, user_id, username, message, message_type, " +
          " server_id, client_ip, timestamp) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String DEAD_LETTER_SQL =
      "INSERT INTO dead_letters (raw_json, error_message) VALUES (?, ?)";

  // ── State ──
  private final HikariDataSource dataSource;
  private final BlockingQueue<QueueMessage> buffer = new LinkedBlockingQueue<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final ExecutorService writerPool;

  // ── Metrics ──
  private final AtomicLong totalWritten   = new AtomicLong(0);
  private final AtomicLong totalErrors    = new AtomicLong(0);
  private final AtomicLong totalBatches   = new AtomicLong(0);

  public DatabaseWriter(String host, String database, String user, String password) {
    // Set up HikariCP connection pool
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql://" + host + ":3306/" + database +
        "?useSSL=false&allowPublicKeyRetrieval=true" +
        "&rewriteBatchedStatements=true");
    config.setUsername(user);
    config.setPassword(password);
    config.setMaximumPoolSize(POOL_SIZE);
    config.setMinimumIdle(POOL_SIZE / 2);
    config.setConnectionTimeout(5000);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    this.dataSource = new HikariDataSource(config);

    // Writer thread pool
    this.writerPool = Executors.newFixedThreadPool(WRITER_THREADS);

    // Periodic flush timer
    scheduler.scheduleAtFixedRate(this::flushBuffer,
        FLUSH_INTERVAL, FLUSH_INTERVAL, TimeUnit.MILLISECONDS);

    System.out.println("[DatabaseWriter] Initialized — batch=" + BATCH_SIZE +
        ", flush=" + FLUSH_INTERVAL + "ms, pool=" + POOL_SIZE);
  }

  /**
   * Called by ConsumerWorker for every message consumed from RabbitMQ.
   * Non-blocking — just adds to the buffer.
   */
  public void addMessage(QueueMessage msg) {
    buffer.offer(msg);

    // If buffer reaches batch size, trigger an immediate flush
    if (buffer.size() >= BATCH_SIZE) {
      flushBuffer();
    }
  }

  /**
   * Drain up to BATCH_SIZE messages from the buffer and write them.
   */
  private void flushBuffer() {
    List<QueueMessage> batch = new ArrayList<>(BATCH_SIZE);
    buffer.drainTo(batch, BATCH_SIZE);
    if (batch.isEmpty()) return;

    // Submit the batch write to the writer pool (async)
    writerPool.submit(() -> writeBatch(batch));
  }

  /**
   * Perform the actual batch INSERT IGNORE into MySQL.
   */
  private void writeBatch(List<QueueMessage> batch) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {

      conn.setAutoCommit(false);

      for (QueueMessage msg : batch) {
        ps.setString(1, msg.messageId);
        ps.setString(2, msg.roomId);
        ps.setString(3, msg.userId);
        ps.setString(4, msg.username);
        ps.setString(5, msg.message);
        ps.setString(6, msg.messageType);
        ps.setString(7, msg.serverId);
        ps.setString(8, msg.clientIp);
        ps.setTimestamp(9, parseTimestamp(msg.timestamp));
        ps.addBatch();
      }

      ps.executeBatch();
      conn.commit();

      long written = totalWritten.addAndGet(batch.size());
      totalBatches.incrementAndGet();

      if (written % 10000 == 0) {
        System.out.println("[DatabaseWriter] Total written: " + written +
            " | Batches: " + totalBatches.get() +
            " | Errors: " + totalErrors.get() +
            " | Buffer: " + buffer.size());
      }

    } catch (Exception e) {
      totalErrors.addAndGet(batch.size());
      System.err.println("[DatabaseWriter] Batch write failed (" +
          batch.size() + " msgs): " + e.getMessage());

      // Save failed messages to dead letter table
      saveToDeadLetters(batch, e.getMessage());
    }
  }

  /**
   * Save failed messages to dead_letters table for retry later.
   */
  private void saveToDeadLetters(List<QueueMessage> batch, String error) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(DEAD_LETTER_SQL)) {

      for (QueueMessage msg : batch) {
        ps.setString(1, msg.messageId + "|" + msg.roomId + "|" + msg.message);
        ps.setString(2, error);
        ps.addBatch();
      }
      ps.executeBatch();

    } catch (Exception ex) {
      System.err.println("[DatabaseWriter] Dead letter save also failed: " + ex.getMessage());
    }
  }

  private Timestamp parseTimestamp(String isoTimestamp) {
    try {
      return Timestamp.from(Instant.parse(isoTimestamp));
    } catch (Exception e) {
      return new Timestamp(System.currentTimeMillis());
    }
  }

  // ── Metrics getters (used by Metrics API) ──

  public long getTotalWritten() { return totalWritten.get(); }
  public long getTotalErrors()  { return totalErrors.get(); }
  public long getTotalBatches() { return totalBatches.get(); }
  public int  getBufferSize()   { return buffer.size(); }
  public int  getPoolSize()     { return POOL_SIZE; }

  /**
   * Graceful shutdown — flush remaining messages before stopping.
   */
  public void shutdown() {
    System.out.println("[DatabaseWriter] Shutting down, flushing remaining " + buffer.size() + " messages...");
    scheduler.shutdown();

    // Flush everything left in the buffer
    List<QueueMessage> remaining = new ArrayList<>();
    buffer.drainTo(remaining);
    if (!remaining.isEmpty()) {
      writeBatch(remaining);
    }

    writerPool.shutdown();
    try {
      writerPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    dataSource.close();
    System.out.println("[DatabaseWriter] Shutdown complete. Total written: " + totalWritten.get());
  }
}