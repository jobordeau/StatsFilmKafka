package org.esgi.project.streaming.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record View(
        int id,
        String title,
        @JsonProperty("view_category") ViewCategory category) {
}
