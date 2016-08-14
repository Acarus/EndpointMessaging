package com.acarus.messaging.akka;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import io.grpc.stub.StreamObserver;

import static com.acarus.messaging.gen.MessagingProto.EndpointInfo;
import static com.acarus.messaging.gen.MessagingProto.Message;

public class MessageWriterActor extends UntypedActor {

    private final EndpointInfo endpointInfo;
    private final StreamObserver<Message> messageStreamObserver;

    public MessageWriterActor(final EndpointInfo endpointInfo, final StreamObserver<Message> messageStreamObserver) {
        this.endpointInfo = endpointInfo;
        this.messageStreamObserver = messageStreamObserver;
    }

    public static Props props(final EndpointInfo endpointInfo, final StreamObserver<Message> messageStreamObserver) {
        return Props.create(new Creator<MessageWriterActor>() {
            @Override
            public MessageWriterActor create() throws Exception {
                return new MessageWriterActor(endpointInfo, messageStreamObserver);
            }
        });
    }

    @Override
    public void onReceive(Object o) throws Throwable {
        if (o instanceof Message) {
            Message message = (Message) o;
            if (endpointInfo.getId().equals(message.getReceiver())) {
                messageStreamObserver.onNext(message);
            } else {
                // message delivered to a wrong actor
            }
        } else {
            unhandled(o);
        }
    }
}
