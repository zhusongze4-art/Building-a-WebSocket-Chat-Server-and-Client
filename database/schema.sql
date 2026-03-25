-- ============================================
-- CS6650 Assignment 3 - Chat System Database
-- Database: MySQL 8.x
-- ============================================

CREATE DATABASE IF NOT EXISTS chatdb;
USE chatdb;

-- ============================================
-- 1. Core table: messages
-- Stores every chat message for persistence
-- ============================================
CREATE TABLE IF NOT EXISTS messages (
                                        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
                                        message_id      VARCHAR(64)  NOT NULL,       -- UUID from producer (for idempotency)
    room_id         VARCHAR(32)  NOT NULL,
    user_id         VARCHAR(64)  NOT NULL,
    username        VARCHAR(128) NOT NULL,
    message         TEXT         NOT NULL,
    message_type    ENUM('TEXT', 'JOIN', 'LEAVE') NOT NULL DEFAULT 'TEXT',
    server_id       VARCHAR(64),
    client_ip       VARCHAR(45),
    timestamp       DATETIME(3)  NOT NULL,        -- millisecond precision
    created_at      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    -- Unique constraint on message_id for idempotent writes (handles duplicates)
    UNIQUE KEY uk_message_id (message_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 2. Indexes for required queries
-- ============================================

-- Query 1: Get messages for a room in time range
-- SELECT * FROM messages WHERE room_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp
CREATE INDEX idx_room_time ON messages (room_id, timestamp);

-- Query 2: Get user's message history
-- SELECT * FROM messages WHERE user_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp
CREATE INDEX idx_user_time ON messages (user_id, timestamp);

-- Query 3: Count active users in time window
-- SELECT COUNT(DISTINCT user_id) FROM messages WHERE timestamp BETWEEN ? AND ?
-- (uses idx_room_time or a full scan on timestamp - adding a dedicated index)
CREATE INDEX idx_timestamp ON messages (timestamp);

-- Query 4: Get rooms user has participated in
-- SELECT DISTINCT room_id, MAX(timestamp) FROM messages WHERE user_id = ? GROUP BY room_id
-- (covered by idx_user_time)

-- ============================================
-- 3. Summary table for analytics (optional optimization)
-- Pre-aggregated stats updated periodically
-- ============================================
CREATE TABLE IF NOT EXISTS room_stats (
                                          room_id         VARCHAR(32) NOT NULL,
    minute_bucket   DATETIME    NOT NULL,         -- truncated to minute
    message_count   INT         NOT NULL DEFAULT 0,
    unique_users    INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, minute_bucket)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 4. Dead letter table for failed writes
-- ============================================
CREATE TABLE IF NOT EXISTS dead_letters (
                                            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
                                            raw_json        TEXT NOT NULL,
                                            error_message   VARCHAR(512),
    failed_at       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    retried         BOOLEAN NOT NULL DEFAULT FALSE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;