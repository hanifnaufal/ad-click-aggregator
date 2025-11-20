package com.example.adaggregator.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import java.math.BigDecimal;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

@Data
@EqualsAndHashCode(callSuper = true)
public class ConversionEvent extends Event {
    @NotNull
    private String type;
    
    @NotNull
    @Positive
    private BigDecimal value;
    
    @NotNull
    private String source;
}
