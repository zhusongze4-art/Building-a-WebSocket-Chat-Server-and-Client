# Performance Analysis Results

This document summarizes the performance results of the WebSocket chat system under load,
including throughput, latency statistics, message distribution, and per-room behavior.
All tests were executed against the locally deployed WebSocket server.

---

## Test Environment

- **Client**: Java multi-threaded WebSocket client
- **Server**: Java WebSocket chat server
- **Protocol**: WebSocket
- **Host**: localhost
- **WebSocket Port**: 8080
- **Health Check Port**: 8081
- **Total Messages Sent**: 500,000
- **Rooms**: 20
- **Connections per Room**: 4

---

## Part 1: Throughput Test (Basic Metrics)

The warm-up phase was used to establish stable connections before the main load test.

### Warm-up Phase
- Successful messages: 32,000
- Failed messages: 0
- Total runtime: 0.621 s
- Throughput: **51,503.61 messages/sec**
- Total connections created: 129
- Reconnections: 0

### Main Phase
- Successful messages: 468,000
- Failed messages: 0
- Total runtime: 3.571 s
- Throughput: **131,053.62 messages/sec**
- Total connections created: 128
- Reconnections: 0

**Observation:**  
The client achieves high throughput with zero failures and no reconnections,
indicating stable connection management and effective thread utilization.

---

## Part 2: Latency and Detailed Metrics

Latency was measured per message using send timestamps and acknowledgment timestamps.

### Latency Statistics (ms)

- Total samples (acked): 500,000
- Mean latency: **851.50 ms**
- Median latency: **932 ms**
- P95 latency: **1,331 ms**
- P99 latency: **1,372 ms**
- Minimum latency: **1 ms**
- Maximum latency: **1,454 ms**

**Observation:**  
Latency remains stable under heavy load. The tight gap between P95 and P99
indicates low tail latency variance.

---

## Message Type Distribution

| Message Type | Count |
|--------------|-------|
| TEXT         | 450,130 |
| JOIN         | 25,031 |
| LEAVE        | 24,839 |

The distribution matches the expected workload pattern, with TEXT messages dominating traffic.

---

## Throughput per Room (Message Count)

Each room handled a roughly equal number of messages, indicating effective load balancing:

- Room counts range approximately between **24,600 – 25,400 messages**
- No room shows abnormal overload or starvation

**Observation:**  
Round-robin connection selection per room distributes load evenly across rooms.

---

## Throughput Over Time

Throughput was measured in 10-second buckets (messages/sec):
bucketStartSec,msgPerSec
0,34154.00
1,73212.00
2,120050.00
3,242876.00
4,29708.00

**Observation:**  
Throughput ramps up quickly after warm-up, peaks during the main execution phase,
and decreases naturally as the workload completes.

---

## Summary

- The system sustains **130k+ messages/sec** with zero failures.
- Latency remains within acceptable bounds under high concurrency.
- Message load is evenly distributed across rooms.
- The client design scales efficiently and demonstrates stable WebSocket connection management.

These results confirm that the system meets the performance and scalability goals
outlined in the assignment.

