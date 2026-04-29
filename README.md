# 🎬 StatFilmKafka — Real-Time Movie Streaming Statistics

[![Java](https://img.shields.io/badge/Java-17-orange)](https://openjdk.org/)
[![Kafka Streams](https://img.shields.io/badge/Kafka%20Streams-4.0-black)](https://kafka.apache.org/documentation/streams/)
[![Akka HTTP](https://img.shields.io/badge/Akka%20HTTP-10.7-blue)](https://doc.akka.io/docs/akka-http/current/)
[![Maven](https://img.shields.io/badge/Maven-3.9%2B-red)](https://maven.apache.org/)

A real-time analytics service built on **Apache Kafka Streams** that processes a live feed of movie viewing events and ratings, then exposes the aggregated statistics through a **REST API** and a small web dashboard.

This project was developed as part of the *Mastère Big Data & Intelligence Artificielle* at **ESGI**, around the **KazaaMovies** scenario: a streaming platform whose backend pushes raw viewing and rating events into Kafka topics. The job of this service is to turn those raw events into useful statistics — total views per movie, breakdowns by completion category, real-time activity over the last 5 minutes, and top/flop rankings.

---

## 📑 Table of contents

- [Architecture](#-architecture)
- [Tech stack](#-tech-stack)
- [Project structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Quick start](#-quick-start)
- [REST API](#-rest-api)
- [Web dashboard](#-web-dashboard)
- [Streaming topology in detail](#-streaming-topology-in-detail)
- [Tests](#-tests)
- [Configuration](#-configuration)
- [Troubleshooting](#-troubleshooting)

---

## 🏗 Architecture

```
            ┌───────────────────────┐
            │  movies-view-injector │  (Docker container, simulator)
            │   produces events     │
            └───────────┬───────────┘
                        │
            ┌───────────┴───────────┐
            │                       │
        ┌───▼────┐              ┌───▼────┐
        │ views  │              │ likes  │   Kafka topics (KRaft broker)
        └───┬────┘              └───┬────┘
            │                       │
            └───────────┬───────────┘
                        │
              ┌─────────▼──────────┐
              │  Kafka Streams app │
              │ (StatFilmKafka)    │
              │                    │
              │ - Aggregations     │
              │ - Tumbling windows │
              │ - State stores     │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  Akka HTTP server  │
              │  REST API + UI     │
              └─────────┬──────────┘
                        │
                ┌───────▼────────┐
                │  HTTP clients  │
                │ (browser, curl)│
                └────────────────┘
```

The application maintains **three state stores** populated by the streaming topology, then serves them through queryable stores via the REST API:

| Store                              | Type            | Purpose                                              |
| ---------------------------------- | --------------- | ---------------------------------------------------- |
| `view-stats-all-time-store`        | `KeyValueStore` | Cumulative per-movie view counts since launch        |
| `view-stats-per-minute-store`      | `WindowStore`   | View counts per 1-minute tumbling window (5-min view) |
| `score-stats-store`                | `KeyValueStore` | Sum + count of ratings per movie (for averaging)     |

---

## 🧰 Tech stack

- **Java 17** (records, switch expressions)
- **Apache Kafka Streams 4.0** — stream processing engine
- **Akka HTTP 10.7** (Java DSL) — REST API server
- **Jackson 2.17** — JSON (de)serialization
- **JUnit 5** + **kafka-streams-test-utils** — unit testing
- **Maven Shade Plugin** — fat JAR packaging
- **Docker Compose** — local Kafka cluster + event injector

---

## 📁 Project structure

```
StatFilmKafka/
├── docker-compose.yml             # Kafka (KRaft) + event injector
├── pom.xml                        # Maven build, Java 17
├── README.md
├── .gitignore
└── src/
    ├── main/
    │   ├── java/org/esgi/project/
    │   │   ├── Main.java                              # entrypoint
    │   │   ├── streaming/
    │   │   │   ├── StreamProcessing.java              # topology builder
    │   │   │   ├── models/
    │   │   │   │   ├── View.java                      # input event
    │   │   │   │   ├── Like.java                      # input event
    │   │   │   │   ├── ViewCategory.java              # enum: START_ONLY/HALF/FULL
    │   │   │   │   ├── ViewStats.java                 # aggregator for views
    │   │   │   │   └── ScoreStats.java                # aggregator for scores
    │   │   │   └── serdes/
    │   │   │       ├── JsonPojoSerde.java
    │   │   │       ├── JsonPojoSerializer.java
    │   │   │       └── JsonPojoDeserializer.java
    │   │   └── api/
    │   │       ├── ApiServer.java                     # Akka HTTP bootstrap
    │   │       ├── controllers/MovieController.java   # REST routes
    │   │       ├── services/MovieStatsService.java    # store queries + sorting
    │   │       └── dto/
    │   │           ├── MovieDetailsResponse.java
    │   │           ├── ScoreRankingItem.java
    │   │           └── ViewRankingItem.java
    │   └── resources/
    │       └── public/index.html                      # web dashboard
    └── test/java/org/esgi/project/
        ├── streaming/StreamProcessingTopologyTest.java
        └── api/MovieStatsServiceTest.java
```

---

## ✅ Prerequisites

You only need three things on your machine:

| Tool             | Version    | Check with                |
| ---------------- | ---------- | ------------------------- |
| **JDK**          | 17 or 21   | `java -version`           |
| **Maven**        | 3.9+       | `mvn -version`            |
| **Docker** + Compose | recent  | `docker compose version`  |

> The Docker stack provides a single-node **Kafka 3.7 broker in KRaft mode** (no Zookeeper) plus the **`nekonyuu/movies-view-injector`** image that continuously publishes simulated events to the `views` and `likes` topics.

---

## 🚀 Quick start

### 1. Clone the repository

```bash
git clone <your-repo-url> StatFilmKafka
cd StatFilmKafka
```

### 2. Start Kafka and the event injector

```bash
docker compose up -d
```

This launches:
- `movie-stats-broker` — Kafka broker on `localhost:9092`
- `movie-stats-injector` — pushes events to the `views` and `likes` topics

Verify the containers are running:

```bash
docker compose ps
```

You can also peek at the events being produced:

```bash
docker exec -it movie-stats-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic views --from-beginning --max-messages 5
```

### 3. Build the project

```bash
mvn clean package
```

This compiles the code, runs all the unit tests, and produces a runnable fat JAR at `target/movie-stream-stats.jar`.

### 4. Run the application

```bash
java -jar target/movie-stream-stats.jar
```

You should see the Kafka Streams app start, then the API log:

```
API listening on http://0.0.0.0:8080/
```

### 5. Use the API

Open the dashboard in your browser:

> **<http://localhost:8080>**

Or query the API directly with `curl`:

```bash
curl http://localhost:8080/stats/ten/best/views | jq .
curl http://localhost:8080/movies/1 | jq .
```

### 6. Shutdown

```bash
# Stop the Java app with Ctrl+C, then:
docker compose down
```

---

## 🌐 REST API

All responses are JSON. The base URL during local development is `http://localhost:8080`.

### `GET /movies/:id`

Returns the cumulative + last-5-minutes view breakdown for a single movie.

```bash
curl http://localhost:8080/movies/1
```

```json
{
  "id": 1,
  "title": "Kill Bill",
  "total_view_count": 200,
  "stats": {
    "past": {
      "start_only": 100,
      "half": 10,
      "full": 90
    },
    "last_five_minutes": {
      "start_only": 5,
      "half": 2,
      "full": 8
    }
  }
}
```

Returns **`404 Not Found`** if the movie has not received any view events yet.

### `GET /stats/ten/best/score`

Top 10 movies by average rating, sorted **descending**.

```json
[
  { "id": 99, "title": "movie title 1", "score": 9.98 },
  { "id": 32, "title": "movie title 2", "score": 9.7 }
]
```

### `GET /stats/ten/best/views`

Top 10 most-watched movies, sorted **descending**.

```json
[
  { "id": 12, "title": "movie title 1", "views": 3500 },
  { "id":  2, "title": "movie title 2", "views": 2800 }
]
```

### `GET /stats/ten/worst/score`

10 worst-rated movies, sorted **ascending**.

### `GET /stats/ten/worst/views`

10 least-watched movies, sorted **ascending**.

---

## 🖥 Web dashboard

A lightweight single-page dashboard ships with the application and is served at `/`. It consumes the REST API directly:

- Browse the four ranking views from the navigation bar
- Look up any movie by ID using the input field on the right
- See the all-time vs. last-5-minutes breakdown side by side

No build step is required — it's plain HTML/CSS/JS bundled in the JAR's resources.

---

## 🌊 Streaming topology in detail

### Input events

The injector publishes JSON to two Kafka topics with `null` keys:

```json
// topic: views
{ "id": 1, "title": "Kill Bill", "view_category": "half" }
```

```json
// topic: likes
{ "id": 1, "score": 4.8 }
```

`view_category` is one of `start_only`, `half`, `full` — corresponding to the user stopping at the very beginning, in the middle, or watching to the end.

### Topology stages

Both topics are first **re-keyed** by `movie_id`, since events arrive with no key.

```
views ──► selectKey(movie_id) ──┬──► groupByKey ──► aggregate ──► view-stats-all-time-store
                                │
                                └──► groupByKey ──► windowedBy(1min)
                                                     ──► aggregate ──► view-stats-per-minute-store

likes ──► selectKey(movie_id) ──► groupByKey ──► aggregate ──► score-stats-store
```

### Why 1-minute tumbling windows for the "last 5 minutes" stat?

The naïve approach would be a single 5-minute tumbling window. The problem: when the API is queried at, say, `12:07`, the range `[12:02, 12:07]` overlaps **two** 5-minute windows (`[12:00, 12:05]` and `[12:05, 12:10]`). Summing them would inflate the counts to up to 10 minutes of data.

Using **1-minute tumbling windows** and summing the most recent 5 keeps the answer bounded to a fresh, correctly-sized slice. The tradeoff is finer-grained state, which is fine for the data volumes involved here. Window retention is set to **30 minutes** to cap state-store size.

### State stores and queryable services

Each aggregation materializes a state store. The HTTP layer reads them through Kafka Streams' **interactive queries** API, which is exactly what makes the REST endpoints possible without an external database.

`MovieStatsService` is wired with `Supplier<...Store>` rather than receiving a `KafkaStreams` instance directly. This inversion lets unit tests pass in stores from a `TopologyTestDriver` instead of a live cluster.

---

## 🧪 Tests

The test suite covers the streaming topology and the service layer:

```bash
mvn test
```

What's covered:

| Test class                          | Focus                                                            |
| ----------------------------------- | ---------------------------------------------------------------- |
| `StreamProcessingTopologyTest`      | View aggregation per category, multi-movie isolation, windowing, score averaging, missing-key behavior, title capture |
| `MovieStatsServiceTest`             | Movie details (404 + happy path), top-N ascending/descending for both views and scores, limit enforcement, score rounding |

Both classes use `TopologyTestDriver` from `kafka-streams-test-utils`, which lets the topology run synchronously in-process — no Docker, no broker, fast.

---

## ⚙️ Configuration

The application reads three optional environment variables; sensible defaults apply when they are not set:

| Variable                   | Default            | Description                          |
| -------------------------- | ------------------ | ------------------------------------ |
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`   | Kafka broker(s) to connect to        |
| `HTTP_HOST`                | `0.0.0.0`          | Bind address for the API server      |
| `HTTP_PORT`                | `8080`             | Port for the API server              |

Example for a non-default broker:

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9092 java -jar target/movie-stream-stats.jar
```

Other Kafka Streams settings (auto offset reset, replication factor, application id) are defined in `Main.java`. The application uses **`auto.offset.reset=earliest`** so that restarting the app picks up events that were produced while it was down.

---

## 🩹 Troubleshooting

**The API returns `500` immediately after startup.**
Kafka Streams takes a few seconds to transition to `RUNNING` and warm up its state stores. Wait for the log line `API listening on http://0.0.0.0:8080/` and then a few more seconds before querying.

**`Connection refused` when starting the JAR.**
Make sure the broker is up: `docker compose ps`. If it's healthy but you still can't connect, check that nothing else is bound to `localhost:9092`.

**Empty rankings even though events are flowing.**
Inspect the topics directly:
```bash
docker exec -it movie-stats-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic views --from-beginning --max-messages 5
```
If you see events here but not in the API, double-check that the JAR is connected to the right broker (`KAFKA_BOOTSTRAP_SERVERS`).

**Port `8080` is already in use.**
Run with a different port:
```bash
HTTP_PORT=9090 java -jar target/movie-stream-stats.jar
```

**Want a clean slate.**
The Kafka Streams app keeps its local state in `/tmp/kafka-streams/movie-stats-processor/`. Delete that directory (and run `docker compose down -v` to wipe the broker) to fully reset.

---

## 📜 License

Educational project — ESGI Mastère Big Data & IA.
