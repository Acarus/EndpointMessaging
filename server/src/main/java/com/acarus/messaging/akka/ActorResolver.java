package com.acarus.messaging.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ActorResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ActorResolver.class);

    private static final int MEMCACHED_ITEM_EXPIRATION_TIME = 100 * 60 * 60;

    private final ActorSystem actorSystem;
    private MemcachedClient memcachedClient;

    public ActorResolver(ActorSystem actorSystem, String memchachedHost, int memchachedPort) throws IOException {
        this.actorSystem = actorSystem;
        memcachedClient = new MemcachedClient(new InetSocketAddress(memchachedHost, memchachedPort));
    }

    public synchronized boolean addActor(String endpoint, ActorRef actorRef) {
        String actorPath = Serialization.serializedActorPath(actorRef);
        LOG.debug("Actor: {} is able to write messages to endpoint: {}", actorPath, endpoint);
        OperationFuture<Boolean> status = memcachedClient.set(endpoint, MEMCACHED_ITEM_EXPIRATION_TIME, actorPath);
        try {
            return status.get();
        } catch (Exception e) {
            LOG.error("Can not store actor/endpoint mapping in Memcached");
            return false;
        }
    }

    public synchronized ActorRef getActor(String endpoint) {
        LOG.debug("Looking for an actor which is responsible for endpoint: {}", endpoint);
        String actorPath = (String) memcachedClient.get(endpoint);
        LOG.debug("Found actorPath: {}", actorPath);
        return actorSystem.provider().resolveActorRef(actorPath);
    }

    public void close() {
        LOG.debug("Closing ActorResolver");
        if (memcachedClient != null) {
            memcachedClient.shutdown();
        }
    }
}
