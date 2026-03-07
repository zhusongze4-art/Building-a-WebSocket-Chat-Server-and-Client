# WebSocket Chat Server

This module implements the WebSocket chat server for CS6650 Assignment 1.

## Functionality

- Accepts WebSocket connections at:
  ws://localhost:8080/chat/{roomId}

- Supports multiple chat rooms
- Validates incoming JSON messages
- Echoes valid messages back with server timestamp and status
- Provides a health check REST endpoint

Health endpoint:
http://localhost:8081/health

## How to Run

From the project root directory:

```bash
./gradlew :server:run
