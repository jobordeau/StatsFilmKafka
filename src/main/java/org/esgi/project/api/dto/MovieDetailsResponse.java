package org.esgi.project.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MovieDetailsResponse(
        int id,
        String title,
        @JsonProperty("total_view_count") long totalViewCount,
        Stats stats) {

    public record Stats(
            ViewBreakdown past,
            @JsonProperty("last_five_minutes") ViewBreakdown lastFiveMinutes) {
    }

    public record ViewBreakdown(
            @JsonProperty("start_only") long startOnly,
            long half,
            long full) {
    }
}
