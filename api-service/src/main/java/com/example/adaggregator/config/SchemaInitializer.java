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
                conversion_id UUID,
                click_id UUID,
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
    }
}
