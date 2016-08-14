package com.acarus.messaging.grpc;

import akka.actor.ActorSystem;
import com.acarus.messaging.akka.ActorResolver;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class MessageServer {

    private static final String MEMCACHED_HOST = "127.0.0.1";
    private static final int MEMCACHED_PORT = 11211;

    private Server server;

    public MessageServer(int port) throws IOException {
        ActorSystem actorSystem = ActorSystem.create("MessagingActorSystem");
        ActorResolver actorResolver = new ActorResolver(actorSystem, MEMCACHED_HOST, MEMCACHED_PORT);

        server = ServerBuilder.forPort(port)
                .addService(new MessageService(actorSystem, actorResolver))
                .build()
                .start();
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        MessageServer server = new MessageServer(9057);
        server.blockUntilShutdown();
    }
}
