package org.esgi.project.streaming.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum ViewCategory {
    START_ONLY("start_only"),
    HALF("half"),
    FULL("full");

    private final String value;

    ViewCategory(String value) {
        this.value = value;
    }

    @JsonValue
    public String value() {
        return value;
    }

    @JsonCreator
    public static ViewCategory fromValue(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("View category cannot be null");
        }
        for (ViewCategory category : values()) {
            if (category.value.equalsIgnoreCase(raw)) {
                return category;
            }
        }
        throw new IllegalArgumentException("Unknown view category: " + raw);
    }
}
