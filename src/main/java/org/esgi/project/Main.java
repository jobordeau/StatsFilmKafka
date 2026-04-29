package org.esgi.project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.esgi.project.api.ApiServer;
import org.esgi.project.api.services.MovieStatsService;
import org.esgi.project.streaming.StreamProcessing;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class Main {

    private static final String APPLICATION_ID = "movie-stats-processor";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_HTTP_HOST = "0.0.0.0";
    private static final int DEFAULT_HTTP_PORT = 8080;

    private Main() {
    }

    public static void main(String[] args) throws InterruptedException {
        StreamProcessing streamProcessing = new StreamProcessing();
        KafkaStreams streams = new KafkaStreams(streamProcessing.buildTopology(), streamsProperties());

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            shutdownLatch.countDown();
        }, "streams-shutdown-hook"));

        streams.start();

        MovieStatsService service = MovieStatsService.fromKafkaStreams(streams);
        new ApiServer(service, httpHost(), httpPort()).start();

        shutdownLatch.await();
    }

    private static Properties streamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return properties;
    }

    private static String bootstrapServers() {
        return env("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
    }

    private static String httpHost() {
        return env("HTTP_HOST", DEFAULT_HTTP_HOST);
    }

    private static int httpPort() {
        return Integer.parseInt(env("HTTP_PORT", String.valueOf(DEFAULT_HTTP_PORT)));
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
