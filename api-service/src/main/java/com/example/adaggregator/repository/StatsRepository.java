package com.example.adaggregator.repository;

import com.example.adaggregator.model.StatsResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Repository
public class StatsRepository {

    private final JdbcTemplate jdbcTemplate;

    public StatsRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Mono<List<StatsResponse.StatsEntry>> getStats(LocalDate startDate, LocalDate endDate, String campaignId, String interval) {
        return Mono.fromCallable(() -> {
            String timeBucket = switch (interval.toLowerCase()) {
                case "weekly" -> "toStartOfWeek(day)";
                case "monthly" -> "toStartOfMonth(day)";
                default -> "day";
            };

            StringBuilder sql = new StringBuilder("""
                SELECT
                    %s as time_bucket,
                    COALESCE(clicks.campaign_id, conversions.campaign_id) as campaign_id,
                    COALESCE(clicks.source, conversions.source) as source,
                    sum(COALESCE(clicks.total_clicks, 0)) as clicks,
                    sum(COALESCE(conversions.total_conversions, 0)) as conversions,
                    sum(COALESCE(conversions.total_revenue, 0)) as revenue
                FROM daily_clicks_mv AS clicks
                FULL OUTER JOIN (
                    SELECT
                        day,
                        campaign_id,
                        source,
                        sum(total_conversions) as total_conversions,
                        sum(total_revenue) as total_revenue
                    FROM daily_stats_mv
                    WHERE day >= ? AND day <= ?
                    GROUP BY day, campaign_id, source
                ) AS conversions
                ON clicks.day = conversions.day
                   AND clicks.campaign_id = conversions.campaign_id
                   AND clicks.source = conversions.source
                WHERE COALESCE(clicks.day, conversions.day) >= ? AND COALESCE(clicks.day, conversions.day) <= ?
            """.formatted(timeBucket.replace("day", "COALESCE(clicks.day, conversions.day)")));

            List<Object> params = new ArrayList<>();
            params.add(startDate);
            params.add(endDate);
            params.add(startDate);
            params.add(endDate);            

            if (campaignId != null) {
                sql.append(" AND COALESCE(clicks.campaign_id, conversions.campaign_id) = ?");
                params.add(campaignId);
            }

            sql.append(" GROUP BY time_bucket, campaign_id, source");
            sql.append(" ORDER BY time_bucket, campaign_id, source");

            return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> mapRow(rs), params.toArray());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private StatsResponse.StatsEntry mapRow(ResultSet rs) throws SQLException {
        long clicks = rs.getLong("clicks");
        long conversions = rs.getLong("conversions");
        BigDecimal revenue = rs.getBigDecimal("revenue");
        double cvr = clicks > 0 ? (double) conversions / clicks : 0.0;

        return StatsResponse.StatsEntry.builder()
            .date(rs.getDate("time_bucket").toLocalDate())
            .campaignId(rs.getString("campaign_id"))
            .source(rs.getString("source"))
            .clicks(clicks)
            .conversions(conversions)
            .revenue(revenue)
            .cvr(cvr)
            .build();
    }
}
