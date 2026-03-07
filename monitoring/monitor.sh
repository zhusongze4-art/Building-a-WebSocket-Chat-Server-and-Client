#!/bin/bash
# Monitoring script for chat server system
# Usage: ./monitor.sh

RABBITMQ_HOST="3.131.36.170"
RABBITMQ_USER="admin"
RABBITMQ_PASS="admin123"
RABBITMQ_API="http://$RABBITMQ_HOST:15672/api"

SERVER_IPS=("3.15.157.223" "3.145.149.194" "3.23.131.241" "18.218.113.75")

echo "=============================="
echo " Chat System Monitor"
echo " $(date)"
echo "=============================="

# 1. Check RabbitMQ queue depths
echo ""
echo "[RabbitMQ] Queue depths:"
for i in $(seq 1 20); do
  DEPTH=$(curl -s -u $RABBITMQ_USER:$RABBITMQ_PASS \
    "$RABBITMQ_API/queues/%2F/room.$i" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages',0))" 2>/dev/null)
  if [ "$DEPTH" -gt "0" ] 2>/dev/null; then
    echo "  room.$i: $DEPTH messages"
  fi
done

# 2. Check RabbitMQ message rates
echo ""
echo "[RabbitMQ] Message rates:"
curl -s -u $RABBITMQ_USER:$RABBITMQ_PASS \
  "$RABBITMQ_API/overview" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
mr = d.get('message_stats', {})
print('  Publish rate:  ', mr.get('publish_details', {}).get('rate', 0), 'msg/s')
print('  Deliver rate:  ', mr.get('deliver_get_details', {}).get('rate', 0), 'msg/s')
print('  Consumer ack:  ', mr.get('ack_details', {}).get('rate', 0), 'msg/s')
" 2>/dev/null

# 3. Check server health
echo ""
echo "[Servers] Health check:"
for IP in "${SERVER_IPS[@]}"; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://$IP:8081/health)
  if [ "$STATUS" = "200" ]; then
    echo "  $IP: OK"
  else
    echo "  $IP: UNHEALTHY (HTTP $STATUS)"
  fi
done

# 4. Check consumer log (if run on consumer server)
echo ""
echo "[Consumer] Last 5 stats lines:"
if [ -f ~/consumer.log ]; then
  grep "Stats" ~/consumer.log | tail -5
else
  echo "  consumer.log not found (run this on consumer server)"
fi

echo ""
echo "=============================="