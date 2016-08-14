package com.acarus.messaging.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import io.grpc.stub.StreamObserver;

import static com.acarus.messaging.gen.MessagingProto.Message;
import static com.acarus.messaging.gen.MessagingProto.MessageDeliveryStatus;

public class MessageReaderActor extends UntypedActor {

    private final StreamObserver<MessageDeliveryStatus> messageDeliveryStatusStreamObserver;
    private final ActorResolver actorResolver;

    public MessageReaderActor(final StreamObserver<MessageDeliveryStatus> messageDeliveryStatusStreamObserver,
                              final ActorResolver actorResolver) {
        this.messageDeliveryStatusStreamObserver = messageDeliveryStatusStreamObserver;
        this.actorResolver = actorResolver;
    }

    public static Props props(final StreamObserver<MessageDeliveryStatus> messageDeliveryStatusStreamObserver,
                              final ActorResolver actorResolver) {
        return Props.create(new Creator<MessageReaderActor>() {
            @Override
            public MessageReaderActor create() throws Exception {
                return new MessageReaderActor(messageDeliveryStatusStreamObserver, actorResolver);
            }
        });
    }

    @Override
    public void onReceive(Object o) throws Throwable {
        if (o instanceof Message) {
            Message message = (Message) o;
            ActorRef messageWriterActor = actorResolver.getActor(message.getReceiver());
            messageWriterActor.tell(message, getSelf());
        } else if (o instanceof MessageDeliveryStatus) {
            messageDeliveryStatusStreamObserver.onNext((MessageDeliveryStatus) o);
        } else {
            unhandled(o);
        }
    }
}
