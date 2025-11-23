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

    public Mono<List<StatsResponse.StatsEntry>> getDailyStats(LocalDate startDate, LocalDate endDate, String campaignId) {
        return Mono.fromCallable(() -> {
            StringBuilder sql = new StringBuilder("""
                SELECT
                    day,
                    campaign_id,
                    source,
                    sum(total_conversions) as conversions,
                    sum(total_revenue) as revenue
                FROM daily_stats_mv
                WHERE day >= ? AND day <= ?
            """);

            List<Object> params = new ArrayList<>();
            params.add(startDate);
            params.add(endDate);

            if (campaignId != null) {
                sql.append(" AND campaign_id = ?");
                params.add(campaignId);
            }

            sql.append(" GROUP BY day, campaign_id, source");
            sql.append(" ORDER BY day, campaign_id, source");

            return jdbcTemplate.query(sql.toString(), (rs, rowNum) -> mapRow(rs), params.toArray());
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private StatsResponse.StatsEntry mapRow(ResultSet rs) throws SQLException {
        long conversions = rs.getLong("conversions");
        BigDecimal revenue = rs.getBigDecimal("revenue");
        // Clicks are missing from the current schema design.
        long clicks = 0; 
        double cvr = clicks > 0 ? (double) conversions / clicks : 0.0;

        return StatsResponse.StatsEntry.builder()
                .date(rs.getDate("day").toLocalDate())
                .campaignId(rs.getString("campaign_id"))
                .source(rs.getString("source"))
                .clicks(clicks)
                .conversions(conversions)
                .revenue(revenue)
                .cvr(cvr)
                .build();
    }
}
