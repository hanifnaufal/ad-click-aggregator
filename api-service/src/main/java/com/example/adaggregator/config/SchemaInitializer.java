package com.example.adaggregator.config;

import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class SchemaInitializer implements CommandLineRunner {

    // Kafka Configuration
    private static final String KAFKA_BROKER = "kafka:29092";

    private final JdbcTemplate jdbcTemplate;

    public SchemaInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        // ============================================
        // RAW DATA TABLES (MergeTree)
        // These store the actual events for querying
        // ============================================
        
        createClicksTable();
        createAttributedEventsTable();
        
        // ============================================
        // INGESTION PIPELINE (Kafka → ClickHouse)
        // Raw events topic → clicks table
        // ============================================
        
        createRawEventsKafkaConsumer();
        createClicksConsumerMaterializedView();
        
        // ============================================
        // INGESTION PIPELINE (Kafka → ClickHouse)
        // Attributed events topic → attributed_events table
        // ============================================
        
        createAttributedEventsKafkaConsumer();
        createAttributedEventsMaterializedView();
        
        // ============================================
        // AGGREGATION LAYER
        // Pre-computed daily stats for fast querying
        // ============================================
        
        createDailyCombinedStatsMaterializedView();
    }

    // ============================================
    // RAW DATA TABLES
    // ============================================

    private void createClicksTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS clicks (
                event_id String,
                user_id String,
                campaign_id String,
                ad_id String,
                source String,
                click_time DateTime
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(click_time)
            ORDER BY (campaign_id, source, click_time);
        """);
    }

    private void createAttributedEventsTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS attributed_events (
                conversion_id String,
                click_id String,
                user_id String,
                ad_id String,
                campaign_id String,
                source String,
                conversion_type LowCardinality(String),
                value Decimal(18, 2),
                click_time DateTime,
                conversion_time DateTime
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMMDD(conversion_time)
            ORDER BY (campaign_id, ad_id, conversion_time);
        """);
    }

    // ============================================
    // KAFKA CONSUMERS - CLICKS PIPELINE
    // ============================================

    private void createRawEventsKafkaConsumer() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS raw_events_kafka (
                raw_data String
            ) ENGINE = Kafka
            SETTINGS
                kafka_broker_list = '%s',
                kafka_topic_list = 'raw-events',
                kafka_group_name = 'clickhouse_clicks_consumer',
                kafka_format = 'LineAsString',
                kafka_num_consumers = 1;
        """.formatted(KAFKA_BROKER));
    }

    private void createClicksConsumerMaterializedView() {
        jdbcTemplate.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS clicks_consumer_mv TO clicks AS
            SELECT
                visitParamExtractString(raw_data, 'event_id') as event_id,
                visitParamExtractString(raw_data, 'user_id') as user_id,
                visitParamExtractString(raw_data, 'campaign_id') as campaign_id,
                visitParamExtractString(raw_data, 'ad_id') as ad_id,
                visitParamExtractString(raw_data, 'source') as source,
                toDateTime(visitParamExtractUInt(raw_data, 'timestamp') / 1000) as click_time
            FROM raw_events_kafka
            WHERE visitParamExtractString(raw_data, 'event_type') = 'click';
        """);
    }

    // ============================================
    // KAFKA CONSUMERS - ATTRIBUTED EVENTS PIPELINE
    // ============================================

    private void createAttributedEventsKafkaConsumer() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS attributed_events_kafka (
                conversion_id String,
                click_id String,
                user_id String,
                ad_id String,
                campaign_id String,
                source String,
                conversion_type String,
                value Decimal(18, 2),
                click_time UInt64,
                conversion_time UInt64
            ) ENGINE = Kafka
            SETTINGS
                kafka_broker_list = '%s',
                kafka_topic_list = 'attributed-events',
                kafka_group_name = 'clickhouse_attributed_consumer',
                kafka_format = 'JSONEachRow',
                kafka_num_consumers = 1;
        """.formatted(KAFKA_BROKER));
    }

    private void createAttributedEventsMaterializedView() {
        jdbcTemplate.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS attributed_events_mv TO attributed_events AS
            SELECT
                conversion_id,
                click_id,
                user_id,
                ad_id,
                campaign_id,
                source,
                conversion_type,
                value,
                toDateTime(click_time / 1000) as click_time,
                toDateTime(conversion_time / 1000) as conversion_time
            FROM attributed_events_kafka;
        """);
    }

    // ============================================
    // AGGREGATION LAYER
    // ============================================

    private void createDailyCombinedStatsMaterializedView() {
        jdbcTemplate.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS daily_combined_stats_mv
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMMDD(day)
            ORDER BY (campaign_id, source, day)
            POPULATE
            AS SELECT
                day,
                campaign_id,
                source,
                sum(clicks) as clicks,
                sum(conversions) as conversions,
                sum(revenue) as revenue
            FROM (
                SELECT
                    toStartOfDay(click_time) as day,
                    campaign_id,
                    source,
                    count() as clicks,
                    0 as conversions,
                    0 as revenue
                FROM clicks
                GROUP BY day, campaign_id, source
                
                UNION ALL
                
                SELECT
                    toStartOfDay(conversion_time) as day,
                    campaign_id,
                    source,
                    0 as clicks,
                    count() as conversions,
                    sum(value) as revenue
                FROM attributed_events
                GROUP BY day, campaign_id, source
            )
            GROUP BY day, campaign_id, source;
        """);
    }
}
