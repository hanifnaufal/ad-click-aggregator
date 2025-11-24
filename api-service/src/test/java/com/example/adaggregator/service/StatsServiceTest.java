package com.example.adaggregator.service;

import com.example.adaggregator.model.StatsResponse;
import com.example.adaggregator.repository.StatsRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatsServiceTest {

    @Mock
    private StatsRepository statsRepository;

    private StatsService statsService;

    @BeforeEach
    void setUp() {
        statsService = new StatsService(statsRepository);
    }

    @Test
    void getStats_shouldReturnStatsResponse() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";
        String interval = "daily";

        List<StatsResponse.StatsEntry> mockEntries = List.of(
            StatsResponse.StatsEntry.builder()
                .date(startDate)
                .campaignId(campaignId)
                .source("google")
                .clicks(100)
                .conversions(10)
                .revenue(BigDecimal.valueOf(500.00))
                .cvr(0.1)
                .build()
        );

        when(statsRepository.getStats(eq(startDate), eq(endDate), eq(campaignId), eq(interval)))
            .thenReturn(Mono.just(mockEntries));

        StepVerifier.create(statsService.getStats(startDate, endDate, campaignId, interval))
            .expectNextMatches(response ->
                response.getInterval().equals(interval) &&
                response.getData().size() == 1 &&
                response.getData().get(0).getCampaignId().equals(campaignId)
            )
            .verifyComplete();
    }

    @Test
    void getStats_shouldDefaultToDailyInterval() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";

        when(statsRepository.getStats(eq(startDate), eq(endDate), eq(campaignId), eq("daily")))
            .thenReturn(Mono.just(List.of()));

        StepVerifier.create(statsService.getStats(startDate, endDate, campaignId, null))
            .expectNextMatches(response -> response.getInterval().equals("daily"))
            .verifyComplete();
    }
}
