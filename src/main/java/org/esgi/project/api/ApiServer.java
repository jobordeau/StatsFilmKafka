package org.esgi.project.api;

import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import org.esgi.project.api.controllers.MovieController;
import org.esgi.project.api.services.MovieStatsService;

import java.util.concurrent.CompletionStage;

public final class ApiServer extends AllDirectives {

    private static final String ACTOR_SYSTEM_NAME = "movie-stats-api";

    private final MovieStatsService service;
    private final String host;
    private final int port;

    public ApiServer(MovieStatsService service, String host, int port) {
        this.service = service;
        this.host = host;
        this.port = port;
    }

    public CompletionStage<ServerBinding> start() {
        ActorSystem system = ActorSystem.create(ACTOR_SYSTEM_NAME);
        Http http = Http.get(system);

        MovieController controller = new MovieController(service);
        Route routes = concat(controller.createRoute(), staticAssets());

        CompletionStage<ServerBinding> binding = http.newServerAt(host, port).bind(routes);
        binding.thenAccept(b -> system.log().info("API listening on http://{}:{}/", host, port));
        return binding;
    }

    private Route staticAssets() {
        return concat(
                pathSingleSlash(() -> getFromResource("public/index.html")),
                getFromResourceDirectory("public"));
    }
}
