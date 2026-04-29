package org.esgi.project.streaming;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.esgi.project.streaming.models.Like;
import org.esgi.project.streaming.models.ScoreStats;
import org.esgi.project.streaming.models.View;
import org.esgi.project.streaming.models.ViewCategory;
import org.esgi.project.streaming.models.ViewStats;
import org.esgi.project.streaming.serdes.JsonPojoSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StreamProcessingTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], View> viewsTopic;
    private TestInputTopic<byte[], Like> likesTopic;
    private KeyValueStore<Integer, ViewStats> allTimeViewStore;
    private WindowStore<Integer, ViewStats> windowedViewStore;
    private KeyValueStore<Integer, ScoreStats> scoreStore;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-movie-stats");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(new StreamProcessing().buildTopology(), props);

        Serde<View> viewSerde = JsonPojoSerde.of(View.class);
        Serde<Like> likeSerde = JsonPojoSerde.of(Like.class);

        viewsTopic = testDriver.createInputTopic(
                StreamProcessing.VIEWS_TOPIC,
                Serdes.ByteArray().serializer(),
                viewSerde.serializer());

        likesTopic = testDriver.createInputTopic(
                StreamProcessing.LIKES_TOPIC,
                Serdes.ByteArray().serializer(),
                likeSerde.serializer());

        allTimeViewStore = testDriver.getKeyValueStore(StreamProcessing.VIEW_STATS_ALL_TIME_STORE);
        windowedViewStore = testDriver.getWindowStore(StreamProcessing.VIEW_STATS_PER_MINUTE_STORE);
        scoreStore = testDriver.getKeyValueStore(StreamProcessing.SCORE_STATS_STORE);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("All-time aggregation increments the right category counter")
    void aggregatesAllTimeViewsByCategory() {
        viewsTopic.pipeKeyValueList(List.of(
                kv(new View(1, "Dune", ViewCategory.START_ONLY)),
                kv(new View(1, "Dune", ViewCategory.HALF)),
                kv(new View(1, "Dune", ViewCategory.FULL)),
                kv(new View(1, "Dune", ViewCategory.FULL))));

        ViewStats stats = allTimeViewStore.get(1);

        assertNotNull(stats);
        assertEquals("Dune", stats.title);
        assertEquals(4, stats.totalViews);
        assertEquals(1, stats.startOnly);
        assertEquals(1, stats.half);
        assertEquals(2, stats.full);
    }

    @Test
    @DisplayName("Stats are tracked per movie independently")
    void keepsPerMovieStatsIndependent() {
        viewsTopic.pipeKeyValueList(List.of(
                kv(new View(1, "Dune", ViewCategory.FULL)),
                kv(new View(1, "Dune", ViewCategory.FULL)),
                kv(new View(2, "Alien", ViewCategory.HALF))));

        assertEquals(2, allTimeViewStore.get(1).totalViews);
        assertEquals(2, allTimeViewStore.get(1).full);
        assertEquals(1, allTimeViewStore.get(2).totalViews);
        assertEquals(1, allTimeViewStore.get(2).half);
    }

    @Test
    @DisplayName("Tumbling window groups events that share the same minute")
    void aggregatesViewsInsideWindow() {
        long t0 = Instant.parse("2025-06-26T12:00:00Z").toEpochMilli();

        viewsTopic.pipeInput(null, new View(2, "Alien", ViewCategory.FULL), t0);
        viewsTopic.pipeInput(null, new View(2, "Alien", ViewCategory.FULL), t0 + 30_000);
        viewsTopic.pipeInput(null, new View(2, "Alien", ViewCategory.HALF), t0 + Duration.ofMinutes(2).toMillis());

        try (WindowStoreIterator<ViewStats> iterator = windowedViewStore.fetch(
                2,
                Instant.ofEpochMilli(t0),
                Instant.ofEpochMilli(t0 + 60_000))) {
            long total = 0;
            while (iterator.hasNext()) {
                total += iterator.next().value.totalViews;
            }
            assertEquals(2, total);
        }
    }

    @Test
    @DisplayName("Summing the last 5 windows captures only the last 5 minutes")
    void canQueryLastFiveMinutes() {
        long t0 = Instant.parse("2025-06-26T12:00:00Z").toEpochMilli();

        viewsTopic.pipeInput(null, new View(7, "Inception", ViewCategory.FULL), t0);
        viewsTopic.pipeInput(null, new View(7, "Inception", ViewCategory.HALF), t0 + Duration.ofMinutes(1).toMillis());
        viewsTopic.pipeInput(null, new View(7, "Inception", ViewCategory.FULL), t0 + Duration.ofMinutes(3).toMillis());
        viewsTopic.pipeInput(null, new View(7, "Inception", ViewCategory.START_ONLY),
                t0 + Duration.ofMinutes(8).toMillis());

        Instant queryEnd = Instant.ofEpochMilli(t0 + Duration.ofMinutes(4).toMillis());
        Instant queryStart = queryEnd.minus(Duration.ofMinutes(5));

        ViewStats accumulator = new ViewStats();
        try (WindowStoreIterator<ViewStats> iterator = windowedViewStore.fetch(7, queryStart, queryEnd)) {
            while (iterator.hasNext()) {
                accumulator.merge(iterator.next().value);
            }
        }

        assertEquals(3, accumulator.totalViews);
        assertEquals(2, accumulator.full);
        assertEquals(1, accumulator.half);
        assertEquals(0, accumulator.startOnly);
    }

    @Test
    @DisplayName("Score aggregation accumulates total and count for averaging")
    void aggregatesScoresForAveraging() {
        likesTopic.pipeKeyValueList(List.of(
                kv(new Like(3, 8.0)),
                kv(new Like(3, 6.0)),
                kv(new Like(3, 10.0))));

        ScoreStats stats = scoreStore.get(3);

        assertNotNull(stats);
        assertEquals(3, stats.count);
        assertEquals(24.0, stats.totalScore, 0.0001);
        assertEquals(8.0, stats.average(), 0.0001);
    }

    @Test
    @DisplayName("Different movies maintain independent score averages")
    void keepsScoreAveragesPerMovie() {
        likesTopic.pipeKeyValueList(List.of(
                kv(new Like(1, 5.0)),
                kv(new Like(2, 1.0)),
                kv(new Like(2, 3.0))));

        assertEquals(5.0, scoreStore.get(1).average(), 0.0001);
        assertEquals(2.0, scoreStore.get(2).average(), 0.0001);
    }

    @Test
    @DisplayName("Unknown movie ids return null from the stores")
    void returnsNullForUnknownMovie() {
        assertNull(allTimeViewStore.get(999));
        assertNull(scoreStore.get(999));
    }

    @Test
    @DisplayName("Title is captured from the View payload during aggregation")
    void capturesTitleFromViewPayload() {
        viewsTopic.pipeInput(null, new View(42, "The Matrix", ViewCategory.FULL));

        ViewStats stats = allTimeViewStore.get(42);

        assertNotNull(stats);
        assertEquals("The Matrix", stats.title);
    }

    private static <V> KeyValue<byte[], V> kv(V value) {
        return new KeyValue<>(null, value);
    }
}
