package com.acarus.messaging.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ActorResolver {

    private final static int MEMCACHED_ITEM_EXPIRATION_TIME = 900;

    private final ActorSystem actorSystem;
    private MemcachedClient memcachedClient;

    public ActorResolver(ActorSystem actorSystem, String memchachedHost, int memchachedPort) throws IOException {
        this.actorSystem = actorSystem;
        memcachedClient = new MemcachedClient(new InetSocketAddress(memchachedHost, memchachedPort));
    }

    public synchronized boolean addActor(String endpointUUID, ActorRef actorRef) {
        String actorPath = Serialization.serializedActorPath(actorRef);
        OperationFuture<Boolean> status = memcachedClient.set(endpointUUID, MEMCACHED_ITEM_EXPIRATION_TIME, actorPath);
        try {
            return status.get();
        } catch (Exception e) {
            return false;
        }
    }

    public synchronized ActorRef getActor(String endpointUUID) {
        String actorPath = (String) memcachedClient.get(endpointUUID);
        return actorSystem.provider().resolveActorRef(actorPath);
    }

    public void close() {
        if (memcachedClient != null) {
            memcachedClient.shutdown();
        }
    }
}
