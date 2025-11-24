package com.example.adaggregator.controller;

import com.example.adaggregator.model.StatsResponse;
import com.example.adaggregator.service.StatsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@WebFluxTest(StatsController.class)
class StatsControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private StatsService statsService;

    @Test
    void getStats_shouldReturnStats() {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 1, 31);
        String campaignId = "camp-1";
        String interval = "daily";

        StatsResponse mockResponse = StatsResponse.builder()
            .interval(interval)
            .data(List.of(
                StatsResponse.StatsEntry.builder()
                    .date(startDate)
                    .campaignId(campaignId)
                    .source("google")
                    .clicks(100)
                    .conversions(10)
                    .revenue(BigDecimal.valueOf(500.00))
                    .cvr(0.1)
                    .build()
            ))
            .build();

        when(statsService.getStats(eq(startDate), eq(endDate), eq(campaignId), eq(interval)))
            .thenReturn(Mono.just(mockResponse));

        webTestClient.get()
            .uri(uriBuilder -> uriBuilder.path("/api/v1/stats")
                .queryParam("start_date", startDate.toString())
                .queryParam("end_date", endDate.toString())
                .queryParam("campaign_id", campaignId)
                .queryParam("interval", interval)
                .build())
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.interval").isEqualTo(interval)
            .jsonPath("$.data[0].campaignId").isEqualTo(campaignId)
            .jsonPath("$.data[0].clicks").isEqualTo(100);
    }
}
