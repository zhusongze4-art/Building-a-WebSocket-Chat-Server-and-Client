

# Client Part 1 – Basic Load Testing Client

This module implements a multithreaded WebSocket client used for basic load testing and throughput measurement.

## Functionality

- Creates multiple WebSocket connections to the server
- Sends a large volume of chat messages concurrently
- Measures:
  - Total successful messages
  - Failed messages
  - Total runtime
  - Overall throughput (messages per second)

`ClientMainPart1.java` serves as the entry point for **Client Part 1**.

This class is intentionally kept minimal and only delegates execution to `ClientMain`.  
Its purpose is to clearly separate the execution of Part 1 from Part 2 at the entry-point level, as required by the assignment structure.


This client corresponds to Part 1 of the assignment.

## How to Run

Make sure the server is already running.

From the project root directory:

```bash
./gradlew :client-part1:run
