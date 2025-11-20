# Flink Stream Processor - Ad Click Attribution

This Flink job performs real-time attribution of conversion events to click events for ad campaigns. It implements a **last-touch attribution** model with campaign-specific matching within a 24-hour lookback window.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Building](#building)
- [Deployment](#deployment)
- [Testing](#testing)
- [Monitoring](#monitoring)
- [State Management](#state-management)

## Overview

### What It Does

The Flink job consumes events from the `raw-events` Kafka topic and:
1. **Stores clicks** per user and campaign in Flink state
2. **Matches conversions** to clicks from the same campaign within 24 hours
3. **Produces attributed events** to the `attributed-events` Kafka topic
4. **Prevents duplicates** by tracking processed conversion IDs

### Attribution Logic

- **Keying**: Events are keyed by `user_id`
- **Campaign Matching**: Conversions are matched to clicks from the **same campaign**
- **Time Window**: Only clicks within the last 24 hours are considered
- **Last-Touch**: If a user clicks the same campaign multiple times, the **most recent click** is used

## Architecture

### Data Flow

```
raw-events (Kafka)
    ↓
Flink (KeyBy user_id)
    ↓
AttributionProcessFunction
    ├─ Store clicks per campaign
    └─ Match conversions to clicks
    ↓
attributed-events (Kafka)
```

### State Management

The job maintains two types of state per user:

1. **`clicksPerCampaignState`**: `MapState<String, ClickEvent>`
   - Key: `campaign_id`
   - Value: Most recent `ClickEvent` for that campaign

2. **`processedConversionsState`**: `MapState<String, Long>`
   - Key: `conversion_id` (event_id)
   - Value: Timestamp (for deduplication)

## Building

### Prerequisites

- Java 17
- Gradle 8.x (or use included wrapper)

### Build the JAR

```bash
./gradlew clean shadowJar
```

The fat JAR will be located at: `build/libs/flink-processor.jar`

## Deployment

### 1. Start Infrastructure

Ensure Kafka, Zookeeper, and Flink are running:

```bash
# From project root
cd ..
docker-compose up -d
```

### 2. Deploy the Flink Job

```bash
# Copy JAR to JobManager
docker cp build/libs/flink-processor.jar ad-click-aggregator-jobmanager-1:/tmp/

# Submit the job
docker exec ad-click-aggregator-jobmanager-1 flink run -d /tmp/flink-processor.jar
```

or simply run `./dev-push.sh`
Notes: it builds the jar and push it to the jobmanager

### 3. Verify Deployment

- **Flink Web UI**: http://localhost:8081
- **Kafka UI**: http://localhost:8090

Check the Flink Web UI to see the running job and its metrics.