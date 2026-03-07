ALB Configuration
Load Balancer
Name: chat-alb
Type: Application Load Balancer
Scheme: Internet-facing
Region: us-east-2
Listener
Protocol: HTTP
Port: 80
Default action: Forward to chat-servers-tg
Target Group
Name: chat-servers-tg
Protocol: HTTP
Port: 8080
Target type: Instance
Stickiness: Enabled, Load balancer generated cookie, 1 day
Health Check
Protocol: HTTP
Path: /health
Port: 8081
Interval: 30s
Timeout: 5s
Healthy threshold: 2
Unhealthy threshold: 3
Registered Targets
chat-server-1 (original)
chat-server-2
chat-server-3
chat-server-4
Deployment Steps
1. Launch EC2 instances
   bash
# Use chat-server-ami to launch additional instances
# Instance type: t2.micro
# Security group: allow port 22, 8080, 8081
2. Upload and start server on each instance
   bash
   scp -i "key.pem" server/build/libs/server-1.0.jar ec2-user@<IP>:~/
   ssh -i "key.pem" ec2-user@<IP>
   nohup java -jar server-1.0.jar > server.log 2>&1 &
3. Start RabbitMQ (Docker)
   bash
   sudo docker run -d --name rabbitmq \
   -p 5672:5672 -p 15672:15672 \
   -e RABBITMQ_DEFAULT_USER=admin \
   -e RABBITMQ_DEFAULT_PASS=admin123 \
   --restart always \
   rabbitmq:3-management
4. Start Consumer
   bash
   scp -i "key.pem" consumer/build/libs/consumer-1.0.jar ec2-user@<CONSUMER_IP>:~/
   ssh -i "key.pem" ec2-user@<CONSUMER_IP>
   nohup java -jar consumer-1.0.jar > consumer.log 2>&1 &
