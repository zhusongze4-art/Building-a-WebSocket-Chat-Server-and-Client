#!/bin/bash
# ============================================
# CS6650 Assignment 3 - System Monitoring Script
# Collects metrics from Consumer, MySQL, and RabbitMQ
# Usage: ./monitor.sh <consumer-ip> <mysql-ip>
# ============================================

CONSUMER_IP=${1:-"18.118.34.227"}
MYSQL_IP=${2:-"3.131.36.170"}
INTERVAL=10
LOG_FILE="monitoring_$(date +%Y%m%d_%H%M%S).log"

echo "=== CS6650 System Monitor ===" | tee "$LOG_FILE"
echo "Consumer: $CONSUMER_IP | MySQL/RabbitMQ: $MYSQL_IP" | tee -a "$LOG_FILE"
echo "Interval: ${INTERVAL}s | Log: $LOG_FILE" | tee -a "$LOG_FILE"
echo "Started at: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    # 1. Consumer Metrics API
    METRICS=$(curl -s --connect-timeout 5 "http://$CONSUMER_IP:8081/metrics" 2>/dev/null)
    if [ -n "$METRICS" ]; then
        DB_WRITTEN=$(echo "$METRICS" | grep -o '"totalDbWritten":[0-9]*' | cut -d: -f2)
        DB_ERRORS=$(echo "$METRICS" | grep -o '"totalDbErrors":[0-9]*' | cut -d: -f2)
        BUFFER=$(echo "$METRICS" | grep -o '"currentBuffer":[0-9]*' | cut -d: -f2)
        BATCHES=$(echo "$METRICS" | grep -o '"totalBatches":[0-9]*' | cut -d: -f2)
    else
        DB_WRITTEN="N/A"; DB_ERRORS="N/A"; BUFFER="N/A"; BATCHES="N/A"
    fi

    # 2. RabbitMQ Queue Depth
    RABBIT_QUEUES=$(curl -s --connect-timeout 5 -u admin:admin123 \
        "http://$MYSQL_IP:15672/api/queues" 2>/dev/null)
    if [ -n "$RABBIT_QUEUES" ]; then
        TOTAL_QUEUE_DEPTH=$(echo "$RABBIT_QUEUES" | grep -o '"messages":[0-9]*' | \
            cut -d: -f2 | awk '{sum+=$1} END{print sum}')
    else
        TOTAL_QUEUE_DEPTH="N/A"
    fi

    # Log
    echo "$TIMESTAMP | DB_Written=$DB_WRITTEN | DB_Errors=$DB_ERRORS | Buffer=$BUFFER | Batches=$BATCHES | QueueDepth=$TOTAL_QUEUE_DEPTH" | tee -a "$LOG_FILE"

    sleep "$INTERVAL"
done