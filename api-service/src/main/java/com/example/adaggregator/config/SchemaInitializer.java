package com.example.adaggregator.config;

import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class SchemaInitializer implements CommandLineRunner {

    private final JdbcTemplate jdbcTemplate;

    public SchemaInitializer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
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

        jdbcTemplate.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS daily_stats_mv
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMMDD(day)
            ORDER BY (campaign_id, source, conversion_type, day)
            AS SELECT
                toStartOfDay(conversion_time) as day,
                campaign_id,
                source,
                conversion_type,
                count() as total_conversions,
                sum(value) as total_revenue
            FROM attributed_events
            GROUP BY day, campaign_id, source, conversion_type;
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS raw_events_kafka (
                raw_data String
            ) ENGINE = Kafka
            SETTINGS
                kafka_broker_list = 'kafka:29092',
                kafka_topic_list = 'raw-events',
                kafka_group_name = 'clickhouse_clicks_consumer_v2',
                kafka_format = 'LineAsString',
                kafka_num_consumers = 1;
        """);
        
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

        jdbcTemplate.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS daily_clicks_mv
            ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMMDD(day)
            ORDER BY (campaign_id, source, day)
            AS SELECT
                toStartOfDay(click_time) as day,
                campaign_id,
                source,
                count() as total_clicks
            FROM clicks
            GROUP BY day, campaign_id, source;
        """);

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
                kafka_broker_list = 'kafka:29092',
                kafka_topic_list = 'attributed-events',
                kafka_group_name = 'clickhouse_attributed_consumer_v2',
                kafka_format = 'JSONEachRow',
                kafka_num_consumers = 1;
        """);

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
}
