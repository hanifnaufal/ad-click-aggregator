# Ad Click Aggregator
This project is a proof of concept for an ad click aggregator. It is a web application that aggregates ad clicks from multiple sources and provides a single interface for users to click on ads.

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


## Architecture
```mermaid
graph LR
    subgraph "Data Access"
        Client[Mobile/Web Clients] -->|HTTP POST /click| API[API Service]
        Client[Mobile/Web Clients] -->|HTTP POST /conversion| API[API Service]
        Dashboard[Dashboard UI] -->|HTTP GET /stats| API[API Service]
    end
    
    subgraph "Data Processing"
        API -->|topic: raw-clicks| Kafka[Kafka]
        API -->|topic: raw-conversions| Kafka[Kafka]
        Kafka -->|Consume raw data| Flink[Flink]
        Flink --> |topic: attributed-events| Kafka[Kafka]
    end 
    
    subgraph "Data Serving"
        
        Kafka -->|Sink| CH[ClickHouse]
        CH -->|Materialized View| Aggs[Daily Stats]
    end
    
    API[API Service] --> CH[ClickHouse]

```
