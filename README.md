# CS6650 Assignment 1 – WebSocket Chat System

This repository contains the implementation of a WebSocket-based chat server and multithreaded load testing clients, developed for CS6650.

## Repository Structure

- `server/`  
  WebSocket chat server implementation.

- `client-part1/`  
  Basic multithreaded client for load testing and throughput measurement.

- `client-part2/`  
  Extended client with detailed latency measurement and performance analysis.

- `results/`  
  Test outputs, screenshots, CSV metrics, and performance analysis.

## Prerequisites

- Java 17 or later
- Gradle (wrapper included)

## How to Run

### 1. Start the Server
```bash
./gradlew :server:run
