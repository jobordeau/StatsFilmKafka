# StatFilmKafka — Statistiques de streaming de films en temps réel

[![Java](https://img.shields.io/badge/Java-17-orange)](https://openjdk.org/)
[![Kafka Streams](https://img.shields.io/badge/Kafka%20Streams-4.0-black)](https://kafka.apache.org/documentation/streams/)
[![Akka HTTP](https://img.shields.io/badge/Akka%20HTTP-10.7-blue)](https://doc.akka.io/docs/akka-http/current/)
[![Maven](https://img.shields.io/badge/Maven-3.9%2B-red)](https://maven.apache.org/)

Un service d'analyse en temps réel construit sur **Apache Kafka Streams** qui traite un flux en direct d'événements de visionnage de films et de notes, puis expose les statistiques agrégées via une **API REST** et un petit tableau de bord web.

Ce projet a été développé dans le cadre du *Mastère Big Data & Intelligence Artificielle* à **ESGI**, autour du scénario **KazaaMovies** : une plateforme de streaming dont le backend envoie des événements bruts de visionnage et de notation dans des topics Kafka. Le rôle de ce service est de transformer ces événements bruts en statistiques utiles — nombre total de vues par film, répartitions par catégorie de complétion, activité en temps réel sur les 5 dernières minutes, et classements des meilleurs/pires films.

---

## Table des matières

- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Prérequis](#prérequis)
- [Démarrage rapide](#démarrage-rapide)
- [API REST](#api-rest)
- [Tableau de bord web](#tableau-de-bord-web)
- [Topologie de streaming en détail](#topologie-de-streaming-en-détail)
- [Tests](#tests)
- [Configuration](#configuration)
- [Dépannage](#dépannage)

---

## Architecture

```
            ┌───────────────────────┐
            │  movies-view-injector │  (Conteneur Docker, simulateur)
            │   produit des événements
            └───────────┬───────────┘
                        │
            ┌───────────┴───────────┐
            │                       │
        ┌───▼────┐              ┌───▼────┐
        │ views  │              │ likes  │   Topics Kafka (broker KRaft)
        └───┬────┘              └───┬────┘
            │                       │
            └───────────┬───────────┘
                        │
              ┌─────────▼──────────┐
              │  Application       │
              │  Kafka Streams     │
              │ (StatFilmKafka)    │
              │                    │
              │ - Agrégations      │
              │ - Fenêtres         │
              │ - Stockages d'état │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │  Serveur Akka HTTP │
              │  API REST + UI     │
              └─────────┬──────────┘
                        │
                ┌───────▼────────┐
                │  Clients HTTP  │
                │ (navigateur,   │
                │  curl)         │
                └────────────────┘
```

L'application maintient **trois stockages d'état** alimentés par la topologie de streaming, puis les sert via des stores interrogeables via l'API REST :

| Store                              | Type            | Objectif                                                     |
| ---------------------------------- | --------------- | ------------------------------------------------------------ |
| `view-stats-all-time-store`        | `KeyValueStore` | Comptes cumulés de vues par film depuis le lancement         |
| `view-stats-per-minute-store`      | `WindowStore`   | Comptes de vues par fenêtre glissante d'1 minute (vue sur 5 min) |
| `score-stats-store`                | `KeyValueStore` | Somme + nombre de notes par film (pour calculer la moyenne)  |

---

## Stack technique

- **Java 17** (records, expressions switch)
- **Apache Kafka Streams 4.0** — moteur de traitement de flux
- **Akka HTTP 10.7** (Java DSL) — serveur d'API REST
- **Jackson 2.17** — (dé)sérialisation JSON
- **JUnit 5** + **kafka-streams-test-utils** — tests unitaires
- **Maven Shade Plugin** — packaging en fat JAR
- **Docker Compose** — cluster Kafka local + injecteur d'événements

---

## Structure du projet

```
StatFilmKafka/
├── docker-compose.yml             # Kafka (KRaft) + injecteur d'événements
├── pom.xml                        # Build Maven, Java 17
├── README.md
├── .gitignore
└── src/
    ├── main/
    │   ├── java/org/esgi/project/
    │   │   ├── Main.java                              # point d'entrée
    │   │   ├── streaming/
    │   │   │   ├── StreamProcessing.java              # constructeur de topologie
    │   │   │   ├── models/
    │   │   │   │   ├── View.java                      # événement d'entrée
    │   │   │   │   ├── Like.java                      # événement d'entrée
    │   │   │   │   ├── ViewCategory.java              # enum: START_ONLY/HALF/FULL
    │   │   │   │   ├── ViewStats.java                 # agrégateur pour les vues
    │   │   │   │   └── ScoreStats.java                # agrégateur pour les scores
    │   │   │   └── serdes/
    │   │   │       ├── JsonPojoSerde.java
    │   │   │       ├── JsonPojoSerializer.java
    │   │   │       └── JsonPojoDeserializer.java
    │   │   └── api/
    │   │       ├── ApiServer.java                     # bootstrap Akka HTTP
    │   │       ├── controllers/MovieController.java   # routes REST
    │   │       ├── services/MovieStatsService.java    # requêtes sur les stores + tri
    │   │       └── dto/
    │   │           ├── MovieDetailsResponse.java
    │   │           ├── ScoreRankingItem.java
    │   │           └── ViewRankingItem.java
    │   └── resources/
    │       └── public/index.html                      # tableau de bord web
    └── test/java/org/esgi/project/
        ├── streaming/StreamProcessingTopologyTest.java
        └── api/MovieStatsServiceTest.java
```

---

## Prérequis

Vous n'avez besoin que de trois choses sur votre machine :

| Outil            | Version    | Vérifier avec             |
| ---------------- | ---------- | ------------------------- |
| **JDK**          | 17 ou 21   | `java -version`           |
| **Maven**        | 3.9+       | `mvn -version`            |
| **Docker** + Compose | récent  | `docker compose version`  |

> La pile Docker fournit un **broker Kafka 3.7 à nœud unique en mode KRaft** (pas de Zookeeper) ainsi que l'image **`nekonyuu/movies-view-injector`** qui publie en continu des événements simulés vers les topics `views` et `likes`.

---

## Démarrage rapide

### 1. Cloner le dépôt

```bash
git clone <votre-url-de-dépôt> StatFilmKafka
cd StatFilmKafka
```

### 2. Démarrer Kafka et l'injecteur d'événements

```bash
docker compose up -d
```

Cela lance :
- `movie-stats-broker` — broker Kafka sur `localhost:9092`
- `movie-stats-injector` — envoie des événements vers les topics `views` et `likes`

Vérifiez que les conteneurs sont en cours d'exécution :

```bash
docker compose ps
```

Vous pouvez également jeter un œil aux événements produits :

```bash
docker exec -it movie-stats-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic views --from-beginning --max-messages 5
```

### 3. Construire le projet

```bash
mvn clean package
```

Cela compile le code, exécute tous les tests unitaires et produit un fat JAR exécutable dans `target/movie-stream-stats.jar`.

### 4. Lancer l'application

```bash
java -jar target/movie-stream-stats.jar
```

Vous devriez voir l'application Kafka Streams démarrer, puis l'API afficher :

```
API listening on http://0.0.0.0:8080/
```

### 5. Utiliser l'API

Ouvrez le tableau de bord dans votre navigateur :

> **<http://localhost:8080>**

Ou interrogez l'API directement avec `curl` :

```bash
curl http://localhost:8080/stats/ten/best/views | jq .
curl http://localhost:8080/movies/1 | jq .
```

### 6. Arrêt

```bash
# Arrêtez l'application Java avec Ctrl+C, puis :
docker compose down
```

---

## API REST

Toutes les réponses sont en JSON. L'URL de base pendant le développement local est `http://localhost:8080`.

### `GET /movies/:id`

Retourne la répartition cumulative + des 5 dernières minutes pour un seul film.

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

Retourne **`404 Not Found`** si le film n'a encore reçu aucun événement de visionnage.

### `GET /stats/ten/best/score`

Top 10 des films par note moyenne, triés par ordre **décroissant**.

```json
[
  { "id": 99, "title": "titre du film 1", "score": 9.98 },
  { "id": 32, "title": "titre du film 2", "score": 9.7 }
]
```

### `GET /stats/ten/best/views`

Top 10 des films les plus regardés, triés par ordre **décroissant**.

```json
[
  { "id": 12, "title": "titre du film 1", "views": 3500 },
  { "id":  2, "title": "titre du film 2", "views": 2800 }
]
```

### `GET /stats/ten/worst/score`

10 films les moins bien notés, triés par ordre **croissant**.

### `GET /stats/ten/worst/views`

10 films les moins regardés, triés par ordre **croissant**.

---

## Tableau de bord web

Un tableau de bord léger d'une seule page est fourni avec l'application et est servi à `/`. Il consomme l'API REST directement :

- Parcourez les quatre vues de classement depuis la barre de navigation
- Recherchez n'importe quel film par ID en utilisant le champ de saisie à droite
- Visualisez la répartition cumulative vs. 5 dernières minutes côte à côte

Aucune étape de build n'est requise — c'est du HTML/CSS/JS pur intégré dans les ressources du JAR.

---

## Topologie de streaming en détail

### Événements d'entrée

L'injecteur publie du JSON vers deux topics Kafka avec des clés `null` :

```json
// topic: views
{ "id": 1, "title": "Kill Bill", "view_category": "half" }
```

```json
// topic: likes
{ "id": 1, "score": 4.8 }
```

`view_category` peut être `start_only`, `half` ou `full` — correspondant à l'utilisateur arrêtant au tout début, au milieu, ou regardant jusqu'à la fin.

### Étapes de la topologie

Les deux topics sont d'abord **re-clés** par `movie_id`, puisque les événements arrivent sans clé.

```
views ──► selectKey(movie_id) ──┬──► groupByKey ──► aggregate ──► view-stats-all-time-store
                                │
                                └──► groupByKey ──► windowedBy(1min)
                                                     ──► aggregate ──► view-stats-per-minute-store

likes ──► selectKey(movie_id) ──► groupByKey ──► aggregate ──► score-stats-store
```

### Pourquoi des fenêtres glissantes d'1 minute pour la statistique "5 dernières minutes" ?

L'approche naïve serait une seule fenêtre glissante de 5 minutes. Le problème : quand l'API est interrogée à, disons, `12:07`, la plage `[12:02, 12:07]` chevauche **deux** fenêtres de 5 minutes (`[12:00, 12:05]` et `[12:05, 12:10]`). Les additionner gonflerait les comptes jusqu'à 10 minutes de données.

L'utilisation de **fenêtres glissantes d'1 minute** et la sommation des 5 plus récentes maintient la réponse limitée à une tranche fraîche de taille correcte. Le compromis est un état à grain plus fin, ce qui convient aux volumes de données impliqués ici. La rétention des fenêtres est fixée à **30 minutes** pour limiter la taille du store d'état.

### Stockages d'état et services interrogeables

Chaque agrégation matérialise un stockage d'état. La couche HTTP les lit via l'API des **requêtes interactives** de Kafka Streams, ce qui rend possible les endpoints REST sans base de données externe.

`MovieStatsService` est câblé avec `Supplier<...Store>` plutôt que de recevoir une instance `KafkaStreams` directement. Cette inversion permet aux tests unitaires de passer des stores depuis un `TopologyTestDriver` au lieu d'un cluster en direct.

---

## Tests

La suite de tests couvre la topologie de streaming et la couche de service :

```bash
mvn test
```

Ce qui est couvert :

| Classe de test                      | Focus                                                            |
| ----------------------------------- | ---------------------------------------------------------------- |
| `StreamProcessingTopologyTest`      | Agrégation des vues par catégorie, isolation multi-films, fenêtrage, moyenne des scores, comportement sur clés manquantes, capture de titre |
| `MovieStatsServiceTest`             | Détails du film (404 + chemin heureux), top-N croissant/décroissant pour vues et scores, application de limite, arrondi des scores |

Les deux classes utilisent `TopologyTestDriver` de `kafka-streams-test-utils`, qui permet à la topologie de s'exécuter de manière synchrone en processus — pas de Docker, pas de broker, rapide.

---

## Configuration

L'application lit trois variables d'environnement optionnelles ; des valeurs par défaut sensées s'appliquent lorsqu'elles ne sont pas définies :

| Variable                   | Défaut             | Description                          |
| -------------------------- | ------------------ | ------------------------------------ |
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`   | Broker(s) Kafka auxquels se connecter |
| `HTTP_HOST`                | `0.0.0.0`          | Adresse de liaison pour le serveur API |
| `HTTP_PORT`                | `8080`             | Port pour le serveur API              |

Exemple pour un broker non-défaut :

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9092 java -jar target/movie-stream-stats.jar
```

Les autres paramètres de Kafka Streams (réinitialisation automatique d'offset, facteur de réplication, ID d'application) sont définis dans `Main.java`. L'application utilise **`auto.offset.reset=earliest`** afin que le redémarrage de l'application récupère les événements qui ont été produits pendant qu'elle était arrêtée.

---

## Dépannage

**L'API retourne `500` immédiatement après le démarrage.**
Kafka Streams prend quelques secondes pour passer à l'état `RUNNING` et préchauffer ses stockages d'état. Attendez la ligne de log `API listening on http://0.0.0.0:8080/` puis quelques secondes de plus avant d'interroger.

**`Connection refused` lors du démarrage du JAR.**
Assurez-vous que le broker est démarré : `docker compose ps`. S'il est sain mais que vous ne pouvez toujours pas vous connecter, vérifiez que rien d'autre n'est lié à `localhost:9092`.

**Classements vides même si les événements circulent.**
Inspectez les topics directement :
```bash
docker exec -it movie-stats-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic views --from-beginning --max-messages 5
```
Si vous voyez des événements ici mais pas dans l'API, vérifiez que le JAR est connecté au bon broker (`KAFKA_BOOTSTRAP_SERVERS`).

**Le port `8080` est déjà utilisé.**
Lancez avec un port différent :
```bash
HTTP_PORT=9090 java -jar target/movie-stream-stats.jar
```

**Besoin d'une remise à zéro complète.**
L'application Kafka Streams conserve son état local dans `/tmp/kafka-streams/movie-stats-processor/`. Supprimez ce répertoire (et exécutez `docker compose down -v` pour effacer le broker) pour une réinitialisation complète.

---
