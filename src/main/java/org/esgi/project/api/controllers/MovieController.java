package org.esgi.project.api.controllers;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.esgi.project.api.services.MovieStatsService;

import static akka.http.javadsl.server.PathMatchers.integerSegment;

public final class MovieController extends AllDirectives {

    private static final int RANKING_SIZE = 10;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String NOT_FOUND_BODY = "{\"error\":\"Movie not found\"}";

    private final MovieStatsService service;

    public MovieController(MovieStatsService service) {
        this.service = service;
    }

    public Route createRoute() {
        return concat(
                pathPrefix("movies", () ->
                        path(integerSegment(), this::movieDetailsRoute)),
                pathPrefix("stats", () ->
                        pathPrefix("ten", () -> concat(
                                pathPrefix("best", () -> concat(
                                        path("score", () -> get(this::topBestScores)),
                                        path("views", () -> get(this::topBestViews)))),
                                pathPrefix("worst", () -> concat(
                                        path("score", () -> get(this::topWorstScores)),
                                        path("views", () -> get(this::topWorstViews))))))));
    }

    private Route movieDetailsRoute(Integer movieId) {
        return get(() -> service.findMovieDetails(movieId)
                .map(this::asJson)
                .orElseGet(this::notFoundResponse));
    }

    private Route topBestScores() {
        return asJson(service.topByScore(RANKING_SIZE, true));
    }

    private Route topBestViews() {
        return asJson(service.topByViews(RANKING_SIZE, true));
    }

    private Route topWorstScores() {
        return asJson(service.topByScore(RANKING_SIZE, false));
    }

    private Route topWorstViews() {
        return asJson(service.topByViews(RANKING_SIZE, false));
    }

    private Route asJson(Object payload) {
        try {
            String json = MAPPER.writeValueAsString(payload);
            return complete(HttpEntities.create(ContentTypes.APPLICATION_JSON, json));
        } catch (Exception e) {
            return failWith(e);
        }
    }

    private Route notFoundResponse() {
        HttpResponse response = HttpResponse.create()
                .withStatus(StatusCodes.NOT_FOUND)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, NOT_FOUND_BODY));
        return complete(response);
    }
}
