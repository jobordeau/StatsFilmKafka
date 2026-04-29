package org.esgi.project.streaming;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.esgi.project.streaming.models.Like;
import org.esgi.project.streaming.models.ScoreStats;
import org.esgi.project.streaming.models.View;
import org.esgi.project.streaming.models.ViewStats;
import org.esgi.project.streaming.serdes.JsonPojoSerde;

import java.time.Duration;

public final class StreamProcessing {

    public static final String VIEWS_TOPIC = "views";
    public static final String LIKES_TOPIC = "likes";

    public static final String VIEW_STATS_ALL_TIME_STORE = "view-stats-all-time-store";
    public static final String VIEW_STATS_PER_MINUTE_STORE = "view-stats-per-minute-store";
    public static final String SCORE_STATS_STORE = "score-stats-store";

    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    public static final Duration WINDOW_RETENTION = Duration.ofMinutes(30);

    private final StreamsBuilder builder;

    public StreamProcessing() {
        this.builder = new StreamsBuilder();
        defineTopology();
    }

    public Topology buildTopology() {
        return builder.build();
    }

    private void defineTopology() {
        Serde<View> viewSerde = JsonPojoSerde.of(View.class);
        Serde<Like> likeSerde = JsonPojoSerde.of(Like.class);
        Serde<ViewStats> viewStatsSerde = JsonPojoSerde.of(ViewStats.class);
        Serde<ScoreStats> scoreStatsSerde = JsonPojoSerde.of(ScoreStats.class);

        KStream<Integer, View> viewsByMovieId = builder
                .stream(VIEWS_TOPIC, Consumed.with(Serdes.ByteArray(), viewSerde))
                .selectKey((unusedKey, view) -> view.id());

        KStream<Integer, Like> likesByMovieId = builder
                .stream(LIKES_TOPIC, Consumed.with(Serdes.ByteArray(), likeSerde))
                .selectKey((unusedKey, like) -> like.id());

        viewsByMovieId
                .groupByKey(Grouped.with(Serdes.Integer(), viewSerde))
                .aggregate(
                        ViewStats::new,
                        (movieId, view, aggregate) -> aggregate.add(view),
                        Materialized.<Integer, ViewStats, KeyValueStore<Bytes, byte[]>>as(VIEW_STATS_ALL_TIME_STORE)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(viewStatsSerde));

        viewsByMovieId
                .groupByKey(Grouped.with(Serdes.Integer(), viewSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
                .aggregate(
                        ViewStats::new,
                        (windowedKey, view, aggregate) -> aggregate.add(view),
                        Materialized.<Integer, ViewStats, WindowStore<Bytes, byte[]>>as(VIEW_STATS_PER_MINUTE_STORE)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(viewStatsSerde)
                                .withRetention(WINDOW_RETENTION));

        likesByMovieId
                .groupByKey(Grouped.with(Serdes.Integer(), likeSerde))
                .aggregate(
                        ScoreStats::new,
                        (movieId, like, aggregate) -> aggregate.add(like.score()),
                        Materialized.<Integer, ScoreStats, KeyValueStore<Bytes, byte[]>>as(SCORE_STATS_STORE)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(scoreStatsSerde));
    }
}
