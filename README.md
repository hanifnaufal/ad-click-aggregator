# Ad Click Aggregator
This project is a proof of concept for an ad click aggregator. It is a web application that aggregates ad clicks from multiple sources and provides analytical data for ad performance.

## Features
- Receiving click and conversion data
- Attribution logic with 24 hours lookback window
- Deduplication, prevent double counting of conversion
- Real time reporting of clicks, conversion, value, and conversion rate (CVR) on daily, weekly, and monthly bases
- High write thoughtput, low read latency for analytics

## Tech Stack
- Java Spring Boot (Webflux) for Ingestion API
- Apache Kafka as message broker
- Apache Flink for stateful processing for complex attribution logic
- Clickhouse for analytic Database

## System Design
See [docs/system-design.md](docs/system-design.md)

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Java 17+

### 1. Start Infrastructure
Start Kafka, Zookeeper, and Kafka UI using Docker Compose:

```bash
docker-compose up -d
```

- **Kafka UI** will be available at [http://localhost:8090](http://localhost:8090).

### 2. Start API Service
Navigate to the `api-service` directory and run the application:

```bash
cd api-service
./gradlew bootRun
```

The service will start on port `8080`.

### 3. API Usage

#### Send Click Event
```bash
curl -v -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "click",
    "event_id": "evt-1",
    "user_id": "user-123",
    "timestamp": 1678886400000,
    "ad_id": "ad-456",
    "campaign_id": "camp-789",
    "source": "google",
    "metadata": {"browser": "chrome"}
  }'
```

#### Send Conversion Event
```bash
curl -v -X POST http://localhost:8080/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "conversion",
    "event_id": "conv-1",
    "user_id": "user-123",
    "timestamp": 1678890000000,
    "type": "purchase",
    "value": 19.99,
    "source": "ios_app"
  }'
```
