package com.example.adaggregator.repository;

import com.example.adaggregator.model.StatsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatsRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private StatsRepository statsRepository;

    @BeforeEach
    void setUp() {
        statsRepository = new StatsRepository(jdbcTemplate);
    }

    @Test
    void getStats_shouldGenerateCorrectSqlForDailyInterval() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";
        String interval = "daily";

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), anyString()))
            .thenReturn(Collections.emptyList());

        StepVerifier.create(statsRepository.getStats(startDate, endDate, campaignId, interval))
            .expectNext(Collections.emptyList())
            .verifyComplete();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), any(), any(), anyString());

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("day as time_bucket");
        assertThat(sql).contains("GROUP BY time_bucket");
    }

    @Test
    void getStats_shouldGenerateCorrectSqlForWeeklyInterval() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";
        String interval = "weekly";

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), anyString()))
            .thenReturn(Collections.emptyList());

        StepVerifier.create(statsRepository.getStats(startDate, endDate, campaignId, interval))
            .expectNext(Collections.emptyList())
            .verifyComplete();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), any(), any(), anyString());

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("toStartOfWeek(day) as time_bucket");
    }

    @Test
    void getStats_shouldGenerateCorrectSqlForMonthlyInterval() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";
        String interval = "monthly";

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), anyString()))
            .thenReturn(Collections.emptyList());

        StepVerifier.create(statsRepository.getStats(startDate, endDate, campaignId, interval))
            .expectNext(Collections.emptyList())
            .verifyComplete();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), any(), any(), anyString());

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("toStartOfMonth(day) as time_bucket");
    }

    @Test
    void getStats_shouldMapRowCorrectly() throws SQLException {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 1);
        String interval = "daily";

        // Mock ResultSet
        ResultSet rs = mock(ResultSet.class);
        when(rs.getDate("time_bucket")).thenReturn(Date.valueOf(startDate));
        when(rs.getString("campaign_id")).thenReturn("camp-1");
        when(rs.getString("source")).thenReturn("google");
        when(rs.getLong("conversions")).thenReturn(10L);
        when(rs.getBigDecimal("revenue")).thenReturn(BigDecimal.valueOf(100.00));

        // Mock JdbcTemplate to execute the RowMapper
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any()))
            .thenAnswer(invocation -> {
                RowMapper<StatsResponse.StatsEntry> mapper = invocation.getArgument(1);
                return List.of(mapper.mapRow(rs, 1));
            });

        StepVerifier.create(statsRepository.getStats(startDate, endDate, null, interval))
            .expectNextMatches(entries -> {
                StatsResponse.StatsEntry entry = entries.get(0);
                return entry.getDate().equals(startDate) &&
                    entry.getCampaignId().equals("camp-1") &&
                    entry.getConversions() == 10L &&
                    entry.getRevenue().compareTo(BigDecimal.valueOf(100.00)) == 0;
            })
            .verifyComplete();
    }
}
