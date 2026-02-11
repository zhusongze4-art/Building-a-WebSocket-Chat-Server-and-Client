

# Client Part 2 – Performance Analysis Client

This module extends the basic load testing client with detailed performance and latency analysis.

## Functionality

- Measures per-message round-trip latency
- Collects latency statistics:
  - Mean
  - Median
  - 95th percentile
  - 99th percentile
  - Minimum and maximum latency
- Tracks:
  - Message type distribution
  - Throughput per room
  - Throughput over time (time buckets)
- Writes detailed metrics to a CSV file

`ClientMainPart2.java` serves as the entry point for **Client Part 2**.

Similar to Part 1, this class is intentionally lightweight and delegates execution to `ClientMain`.  
The distinction between Part 1 and Part 2 lies in the components enabled during execution, not in duplicated client logic.

This client corresponds to Part 2 and Part 3 (Performance Analysis) of the assignment.

## How to Run

Make sure the server is already running.

From the project root directory:

```bash
./gradlew :client-part2:run
