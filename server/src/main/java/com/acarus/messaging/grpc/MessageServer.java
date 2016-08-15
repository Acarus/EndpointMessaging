package com.acarus.messaging.grpc;

import akka.actor.ActorSystem;
import com.acarus.messaging.akka.ActorResolver;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageServer {

    private static final Logger LOG = LoggerFactory.getLogger(MessageServer.class);

    private static final String MEMCACHED_HOST = "127.0.0.1";
    private static final int MEMCACHED_PORT = 11211;
    private static final int LISTEN_PORT = 9057;

    private Server server;

    public MessageServer(int port) throws IOException {
        LOG.info("Starting server on port: {}", port);
        ActorSystem actorSystem = ActorSystem.create("MessagingActorSystem");
        ActorResolver actorResolver = new ActorResolver(actorSystem, MEMCACHED_HOST, MEMCACHED_PORT);

        server = ServerBuilder.forPort(port)
                .addService(new MessageService(actorSystem, actorResolver))
                .build()
                .start();
        LOG.info("Server is started");
    }

    public void stop() {
        LOG.info("Stopping server");
        if (server != null) {
            server.shutdown();
        }
        LOG.info("Server is stopped");
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = LISTEN_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        MessageServer server = new MessageServer(port);
        server.blockUntilShutdown();
    }
}
