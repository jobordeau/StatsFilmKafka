package org.esgi.project.api.services;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.esgi.project.api.dto.MovieDetailsResponse;
import org.esgi.project.api.dto.ScoreRankingItem;
import org.esgi.project.api.dto.ViewRankingItem;
import org.esgi.project.streaming.StreamProcessing;
import org.esgi.project.streaming.models.ScoreStats;
import org.esgi.project.streaming.models.ViewStats;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public final class MovieStatsService {

    private static final Duration LAST_FIVE_MINUTES = Duration.ofMinutes(5);
    private static final String UNKNOWN_TITLE = "unknown";

    private final Supplier<ReadOnlyKeyValueStore<Integer, ViewStats>> allTimeViewStore;
    private final Supplier<ReadOnlyWindowStore<Integer, ViewStats>> windowedViewStore;
    private final Supplier<ReadOnlyKeyValueStore<Integer, ScoreStats>> scoreStore;
    private final Clock clock;

    public MovieStatsService(
            Supplier<ReadOnlyKeyValueStore<Integer, ViewStats>> allTimeViewStore,
            Supplier<ReadOnlyWindowStore<Integer, ViewStats>> windowedViewStore,
            Supplier<ReadOnlyKeyValueStore<Integer, ScoreStats>> scoreStore,
            Clock clock) {
        this.allTimeViewStore = allTimeViewStore;
        this.windowedViewStore = windowedViewStore;
        this.scoreStore = scoreStore;
        this.clock = clock;
    }

    public static MovieStatsService fromKafkaStreams(KafkaStreams streams) {
        return new MovieStatsService(
                () -> streams.store(StoreQueryParameters.fromNameAndType(
                        StreamProcessing.VIEW_STATS_ALL_TIME_STORE,
                        QueryableStoreTypes.keyValueStore())),
                () -> streams.store(StoreQueryParameters.fromNameAndType(
                        StreamProcessing.VIEW_STATS_PER_MINUTE_STORE,
                        QueryableStoreTypes.windowStore())),
                () -> streams.store(StoreQueryParameters.fromNameAndType(
                        StreamProcessing.SCORE_STATS_STORE,
                        QueryableStoreTypes.keyValueStore())),
                Clock.systemUTC());
    }

    public Optional<MovieDetailsResponse> findMovieDetails(int movieId) {
        ViewStats allTime = allTimeViewStore.get().get(movieId);
        if (allTime == null) {
            return Optional.empty();
        }

        ViewStats lastFive = sumWindowedViews(movieId);

        return Optional.of(new MovieDetailsResponse(
                movieId,
                titleOrFallback(allTime.title),
                allTime.totalViews,
                new MovieDetailsResponse.Stats(
                        new MovieDetailsResponse.ViewBreakdown(allTime.startOnly, allTime.half, allTime.full),
                        new MovieDetailsResponse.ViewBreakdown(lastFive.startOnly, lastFive.half, lastFive.full))));
    }

    public List<ViewRankingItem> topByViews(int limit, boolean descending) {
        Comparator<ViewStats> byViews = Comparator.comparingLong(stats -> stats.totalViews);
        return rankAllTime(limit, descending ? byViews.reversed() : byViews,
                (id, stats) -> new ViewRankingItem(id, titleOrFallback(stats.title), stats.totalViews));
    }

    public List<ScoreRankingItem> topByScore(int limit, boolean descending) {
        Comparator<ScoreStats> byAverage = Comparator.comparingDouble(ScoreStats::average);
        Comparator<ScoreStats> comparator = descending ? byAverage.reversed() : byAverage;
        ReadOnlyKeyValueStore<Integer, ScoreStats> store = scoreStore.get();

        try (KeyValueIterator<Integer, ScoreStats> iterator = store.all()) {
            List<KeyValue<Integer, ScoreStats>> all = drain(iterator);
            return all.stream()
                    .sorted((left, right) -> comparator.compare(left.value, right.value))
                    .limit(limit)
                    .map(kv -> new ScoreRankingItem(
                            kv.key,
                            resolveTitle(kv.key),
                            roundTwoDecimals(kv.value.average())))
                    .toList();
        }
    }

    private <T> List<T> rankAllTime(int limit, Comparator<ViewStats> comparator,
                                    BiFunction<Integer, ViewStats, T> mapper) {
        ReadOnlyKeyValueStore<Integer, ViewStats> store = allTimeViewStore.get();
        try (KeyValueIterator<Integer, ViewStats> iterator = store.all()) {
            List<KeyValue<Integer, ViewStats>> all = drain(iterator);
            return all.stream()
                    .sorted((left, right) -> comparator.compare(left.value, right.value))
                    .limit(limit)
                    .map(kv -> mapper.apply(kv.key, kv.value))
                    .toList();
        }
    }

    private ViewStats sumWindowedViews(int movieId) {
        Instant now = clock.instant();
        Instant from = now.minus(LAST_FIVE_MINUTES);
        ViewStats accumulator = new ViewStats();

        try (WindowStoreIterator<ViewStats> iterator = windowedViewStore.get().fetch(movieId, from, now)) {
            while (iterator.hasNext()) {
                accumulator.merge(iterator.next().value);
            }
        }
        return accumulator;
    }

    private String resolveTitle(int movieId) {
        ViewStats stats = allTimeViewStore.get().get(movieId);
        return stats != null ? titleOrFallback(stats.title) : UNKNOWN_TITLE;
    }

    private static String titleOrFallback(String title) {
        return title != null ? title : UNKNOWN_TITLE;
    }

    private static <K, V> List<KeyValue<K, V>> drain(KeyValueIterator<K, V> iterator) {
        List<KeyValue<K, V>> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    private static double roundTwoDecimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
