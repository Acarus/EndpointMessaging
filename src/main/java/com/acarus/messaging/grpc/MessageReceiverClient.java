package com.acarus.messaging.grpc;

import com.acarus.messaging.gen.MessageProviderGrpc;
import com.acarus.messaging.gen.MessageProviderGrpc.MessageProviderStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.acarus.messaging.gen.MessagingProto.*;

public class MessageReceiverClient {
    private final ManagedChannel channel;
    private final MessageProviderStub asyncStub;

    public MessageReceiverClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();
        asyncStub = MessageProviderGrpc.newStub(channel);
    }

    public static void main(String[] args) {
        MessageReceiverClient client = new MessageReceiverClient("localhost", 9057);
        String endpointId = UUID.randomUUID().toString();
        System.out.println("Endpoint id: " + endpointId);
        client.getMessages(EndpointInfo.newBuilder()
                .setId(endpointId)
                .build());

        Scanner in = new Scanner(System.in);
        in.next();
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getMessages(EndpointInfo endpointInfo) {
        asyncStub.receiveMessages(endpointInfo, new StreamObserver<Message>() {
            @Override
            public void onNext(Message message) {
                try {
                    TextMessage textMessage = TextMessage.parseFrom(message.getData());
                    System.out.println("Sender: " + message.getSender());
                    System.out.println("Text: " + textMessage.getText());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
    }
}
