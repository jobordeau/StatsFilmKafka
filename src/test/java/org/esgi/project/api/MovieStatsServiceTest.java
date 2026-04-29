package org.esgi.project.api.services;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.esgi.project.api.dto.MovieDetailsResponse;
import org.esgi.project.api.dto.ScoreRankingItem;
import org.esgi.project.api.dto.ViewRankingItem;
import org.esgi.project.streaming.StreamProcessing;
import org.esgi.project.streaming.models.Like;
import org.esgi.project.streaming.models.View;
import org.esgi.project.streaming.models.ViewCategory;
import org.esgi.project.streaming.serdes.JsonPojoSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MovieStatsServiceTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], View> viewsTopic;
    private TestInputTopic<byte[], Like> likesTopic;
    private MovieStatsService service;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-service");
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

        service = new MovieStatsService(
                () -> testDriver.getKeyValueStore(StreamProcessing.VIEW_STATS_ALL_TIME_STORE),
                () -> testDriver.getWindowStore(StreamProcessing.VIEW_STATS_PER_MINUTE_STORE),
                () -> testDriver.getKeyValueStore(StreamProcessing.SCORE_STATS_STORE),
                Clock.fixed(Instant.parse("2025-06-26T12:10:00Z"), ZoneOffset.UTC));
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("findMovieDetails returns empty when nothing has been ingested")
    void returnsEmptyForUnknownMovie() {
        assertTrue(service.findMovieDetails(999).isEmpty());
    }

    @Test
    @DisplayName("findMovieDetails exposes all-time totals and the last 5 minutes window")
    void exposesAllTimeAndRecentWindow() {
        long now = Instant.parse("2025-06-26T12:10:00Z").toEpochMilli();
        long beforeWindow = now - Duration.ofMinutes(20).toMillis();
        long insideWindow = now - Duration.ofMinutes(2).toMillis();

        viewsTopic.pipeInput(null, new View(1, "Dune", ViewCategory.START_ONLY), beforeWindow);
        viewsTopic.pipeInput(null, new View(1, "Dune", ViewCategory.FULL), beforeWindow);
        viewsTopic.pipeInput(null, new View(1, "Dune", ViewCategory.HALF), insideWindow);
        viewsTopic.pipeInput(null, new View(1, "Dune", ViewCategory.FULL), insideWindow);

        Optional<MovieDetailsResponse> response = service.findMovieDetails(1);

        assertTrue(response.isPresent());
        MovieDetailsResponse details = response.get();
        assertEquals(1, details.id());
        assertEquals("Dune", details.title());
        assertEquals(4, details.totalViewCount());
        assertEquals(1, details.stats().past().startOnly());
        assertEquals(1, details.stats().past().half());
        assertEquals(2, details.stats().past().full());
        assertEquals(0, details.stats().lastFiveMinutes().startOnly());
        assertEquals(1, details.stats().lastFiveMinutes().half());
        assertEquals(1, details.stats().lastFiveMinutes().full());
    }

    @Test
    @DisplayName("topByViews descending ranks movies from most to least watched")
    void rankByViewsDescending() {
        ingestViews(1, "A", 5);
        ingestViews(2, "B", 10);
        ingestViews(3, "C", 2);

        List<ViewRankingItem> ranking = service.topByViews(10, true);

        assertEquals(3, ranking.size());
        assertEquals(2, ranking.get(0).id());
        assertEquals(10, ranking.get(0).views());
        assertEquals(1, ranking.get(1).id());
        assertEquals(3, ranking.get(2).id());
    }

    @Test
    @DisplayName("topByViews ascending ranks movies from least to most watched")
    void rankByViewsAscending() {
        ingestViews(1, "A", 5);
        ingestViews(2, "B", 10);
        ingestViews(3, "C", 2);

        List<ViewRankingItem> ranking = service.topByViews(10, false);

        assertEquals(3, ranking.get(0).id());
        assertEquals(2, ranking.get(0).views());
        assertEquals(2, ranking.get(2).id());
    }

    @Test
    @DisplayName("topByScore descending orders movies by their average score")
    void rankByScoreDescending() {
        ingestLikes(1, 4.0, 5.0);
        ingestLikes(2, 9.0, 10.0);
        ingestLikes(3, 1.0, 2.0);

        List<ScoreRankingItem> ranking = service.topByScore(10, true);

        assertEquals(3, ranking.size());
        assertEquals(2, ranking.get(0).id());
        assertEquals(9.5, ranking.get(0).score(), 0.0001);
        assertEquals(1, ranking.get(1).id());
        assertEquals(3, ranking.get(2).id());
    }

    @Test
    @DisplayName("topByScore ascending orders movies by lowest average score first")
    void rankByScoreAscending() {
        ingestLikes(1, 4.0, 5.0);
        ingestLikes(2, 9.0, 10.0);
        ingestLikes(3, 1.0, 2.0);

        List<ScoreRankingItem> ranking = service.topByScore(10, false);

        assertEquals(3, ranking.get(0).id());
        assertEquals(1.5, ranking.get(0).score(), 0.0001);
    }

    @Test
    @DisplayName("Rankings respect the requested limit")
    void respectsLimit() {
        for (int i = 1; i <= 15; i++) {
            ingestViews(i, "Movie " + i, i);
        }

        List<ViewRankingItem> ranking = service.topByViews(10, true);

        assertEquals(10, ranking.size());
        assertEquals(15, ranking.get(0).views());
    }

    @Test
    @DisplayName("Score average is rounded to two decimals")
    void roundsAverageScoreToTwoDecimals() {
        ingestLikes(1, 1.0, 2.0, 2.0);

        List<ScoreRankingItem> ranking = service.topByScore(10, true);

        assertFalse(ranking.isEmpty());
        assertEquals(1.67, ranking.get(0).score(), 0.0001);
    }

    private void ingestViews(int movieId, String title, int count) {
        for (int i = 0; i < count; i++) {
            viewsTopic.pipeInput(null, new View(movieId, title, ViewCategory.FULL));
        }
    }

    private void ingestLikes(int movieId, double... scores) {
        for (double score : scores) {
            likesTopic.pipeInput(null, kv(new Like(movieId, score)).value);
        }
    }

    private static <V> KeyValue<byte[], V> kv(V value) {
        return new KeyValue<>(null, value);
    }
}
