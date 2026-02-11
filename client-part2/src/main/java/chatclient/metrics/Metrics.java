package chatclient.metrics;

import java.util.concurrent.atomic.LongAdder;

public class Metrics {
  public final LongAdder success = new LongAdder();
  public final LongAdder failed = new LongAdder();
  public final LongAdder totalConnections = new LongAdder();
  public final LongAdder reconnections = new LongAdder();

  public long startNanos;
  public long endNanos;

  public void start() { startNanos = System.nanoTime(); }
  public void end() { endNanos = System.nanoTime(); }

  public double wallSeconds() { return (endNanos - startNanos) / 1_000_000_000.0; }
}
