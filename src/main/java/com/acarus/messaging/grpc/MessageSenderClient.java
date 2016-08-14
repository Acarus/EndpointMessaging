package com.acarus.messaging.grpc;

import com.acarus.messaging.gen.MessageProviderGrpc;
import com.acarus.messaging.gen.MessageProviderGrpc.MessageProviderStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static com.acarus.messaging.gen.MessagingProto.*;

public class MessageSenderClient {
    private final ManagedChannel channel;
    private final MessageProviderStub asyncStub;

    public MessageSenderClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();
        asyncStub = MessageProviderGrpc.newStub(channel);
    }

    public static void main(String[] args) throws InterruptedException {
        MessageSenderClient client = new MessageSenderClient("localhost", 9057);
        Scanner in = new Scanner(System.in);
        System.out.println("Enter receiver id:");
        String receiver = in.next();

        while (true) {
            System.out.println("Text to send (BREAK to quit):");
            String text = in.next();
            if ("BREAK".equals(text)) {
                break;
            }

            TextMessage message = TextMessage.newBuilder()
                    .setText(text)
                    .build();
            client.sendMessage(message, receiver);
        }
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void sendMessage(TextMessage textMessage, String receiver) {
        Message message = Message.newBuilder()
                .setReceiver(receiver)
                .setSender("sender")
                .setData(textMessage.toByteString())
                .build();

        StreamObserver<Message> messageStreamObserver = asyncStub.sendMessages(new StreamObserver<MessageDeliveryStatus>() {
            @Override
            public void onNext(MessageDeliveryStatus messageDeliveryStatus) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
        messageStreamObserver.onNext(message);
        messageStreamObserver.onCompleted();
    }
}
