package com.example.adaggregator.service;

import com.example.adaggregator.model.StatsResponse;
import com.example.adaggregator.repository.StatsRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.List;

@Service
public class StatsService {

    private final StatsRepository statsRepository;

    public StatsService(StatsRepository statsRepository) {
        this.statsRepository = statsRepository;
    }

    public Mono<StatsResponse> getStats(LocalDate startDate, LocalDate endDate, String campaignId, String interval) {
        return statsRepository.getStats(startDate, endDate, campaignId, interval)
                .map(statsEntries -> StatsResponse.builder()
                        .interval(interval != null ? interval : "daily")
                        .data(statsEntries)
                        .build());
    }
}
