# Load Test Configurations and Results

## Test Environment

- **Client**: Local machine (MacBook Air)
- **WebSocket Servers**: 4x EC2 instances behind ALB
- **Consumer**: EC2 (18.118.34.227) — t2.micro/t3.micro
- **RabbitMQ + MySQL**: EC2 (3.131.36.170) — t2.micro/t3.micro
- **ALB**: chat-alb-230508812.us-east-2.elb.amazonaws.com

## Test 1: Baseline (500K Messages)

### Configuration
| Parameter | Value |
|---|---|
| Total Messages | 500,000 |
| Warmup Threads | 32 |
| Warmup Messages | 32,000 |
| Main Threads | 32 |
| Connections Per Room | 4 |
| Rooms | 20 |
| DB Batch Size | 500 |
| DB Flush Interval | 500ms |
| DB Connection Pool | 10 |
| Consumer Threads | 20 |

### Results
| Metric | Value |
|---|---|
| Successful Messages | 500,000 |
| Failed Messages | 0 |
| Main Phase Runtime | 3.395s |
| Throughput | 137,841 msg/s |
| Latency Mean | 1,060 ms |
| Latency Median | 500 ms |
| Latency P95 | 3,954 ms |
| Latency P99 | 4,707 ms |
| Latency Min | 55 ms |
| Latency Max | 4,836 ms |
| DB Written | 500,000 |
| DB Errors | 0 |
| DB Batches | ~1,000 |

### Message Distribution
- Even distribution across 20 rooms (~25,000 per room)
- Message types: TEXT 90%, JOIN 5%, LEAVE 5%

---

## Test 2: Stress Test (1M Messages)

### Configuration
Same as Test 1, except:
| Parameter | Value |
|---|---|
| Total Messages | 1,000,000 |

### Results
| Metric | Value |
|---|---|
| Successful Messages | 1,000,000 |
| Failed Messages | 0 |
| Main Phase Runtime | 4.015s |
| Throughput | 241,103 msg/s |
| Latency Mean | 1,647 ms |
| Latency Median | 975 ms |
| Latency P95 | 5,258 ms |
| Latency P99 | 5,787 ms |
| Latency Min | 68 ms |
| Latency Max | 5,944 ms |
| DB Written | 1,000,000 |
| DB Errors | 0 |

### Observations
- Throughput improved from 137K to 241K msg/s (75% increase) due to better connection reuse
- Latency increased proportionally with load (median 500ms → 975ms)
- Zero message loss at 1M scale
- Database writer kept up with no errors
- Buffer size remained small (< 500), indicating write-behind pattern working effectively

---

## Bottleneck Analysis

### Primary Bottleneck: Network Latency
- Client-to-ALB round trip dominates latency
- P99 latency ~5.8s at 1M load

### Secondary Bottleneck: Database Writes
- Batch writing at 500 messages/batch keeps up with throughput
- Connection pool of 10 is sufficient for current load

### Not a Bottleneck
- RabbitMQ: Queue depth remained stable and low
- Consumer threads: 20 threads sufficient for 20 rooms
- Memory: No signs of OOM or GC pressure