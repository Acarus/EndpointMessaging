package com.acarus.messaging.grpc;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.acarus.messaging.akka.ActorResolver;
import com.acarus.messaging.akka.MessageReaderActor;
import com.acarus.messaging.akka.MessageWriterActor;
import com.acarus.messaging.gen.MessageProviderGrpc;
import io.grpc.stub.StreamObserver;

import static com.acarus.messaging.gen.MessagingProto.*;

public class MessageService extends MessageProviderGrpc.MessageProviderImplBase {

    private final ActorSystem actorSystem;
    private final ActorResolver actorResolver;

    public MessageService(final ActorSystem actorSystem, ActorResolver actorResolver) {
        this.actorSystem = actorSystem;
        this.actorResolver = actorResolver;
    }

    @Override
    public void receiveMessages(EndpointInfo endpointInfo, StreamObserver<Message> responseObserver) {
        ActorRef messageWriterActor = actorSystem.actorOf(MessageWriterActor.props(endpointInfo, responseObserver));
        // store a path in Memcached
        actorResolver.addActor(endpointInfo.getId(), messageWriterActor);
    }

    @Override
    public StreamObserver<Message> sendMessages(StreamObserver<MessageDeliveryStatus> responseObserver) {
        final ActorRef messageReaderActor = actorSystem.actorOf(MessageReaderActor.props(responseObserver, actorResolver));
        return new StreamObserver<Message>() {
            @Override
            public void onNext(Message message) {
                messageReaderActor.tell(message, null);
            }

            @Override
            public void onError(Throwable throwable) {
                // TODO: implement onError
            }

            @Override
            public void onCompleted() {
                // TODO: implement onCompleted
            }
        };
    }
}
