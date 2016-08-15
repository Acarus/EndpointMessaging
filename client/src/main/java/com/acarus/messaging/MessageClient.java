package com.acarus.messaging;

import com.acarus.messaging.gen.MessageProviderGrpc;
import com.acarus.messaging.gen.MessageProviderGrpc.MessageProviderStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.acarus.messaging.gen.MessagingProto.*;

public class MessageClient {

    private final ManagedChannel channel;
    private final StreamObserver<Message> outgoingMessageStream;
    private final EndpointInfo endpointInfo;
    private ConcurrentHashMap<String, MessageListener> listeners = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Class> classes = new ConcurrentHashMap<>();

    public MessageClient(String host, int port) {
        endpointInfo = EndpointInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();

        MessageProviderStub asyncStub = MessageProviderGrpc.newStub(channel);
        outgoingMessageStream = asyncStub.sendMessages(new MessageDeliveryStatusObserver());
        asyncStub.receiveMessages(endpointInfo, new MessageObserver());
    }

    public EndpointInfo getEndpointInfo() {
        return endpointInfo;
    }

    public <T extends GeneratedMessage> void sendMessage(T msg, String receiver) {
        Message message = Message.newBuilder()
                .setReceiver(receiver)
                .setSender(endpointInfo.getId())
                .setDataClass(msg.getClass().getName())
                .setData(msg.toByteString())
                .build();

        outgoingMessageStream.onNext(message);
    }

    public <T extends GeneratedMessage> void addMessageListener(MessageListener<T> listener, Class<T> clazz) {
        listeners.put(clazz.getName(), listener);
        classes.put(clazz.getName(), clazz);
    }

    public void close() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        if (outgoingMessageStream != null) {
            outgoingMessageStream.onCompleted();
        }
    }

    public interface MessageListener<T extends GeneratedMessage> {
        void onReceive(T message);
    }

    private class MessageObserver implements StreamObserver<Message> {

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(Message message) {
            String messageClassName = message.getDataClass();
            if (MessageClient.this.listeners.containsKey(messageClassName)) {
                Class clazz = classes.get(messageClassName);
                MessageListener listener = listeners.get(messageClassName);
                try {
                    Method parseFrom = clazz.getDeclaredMethod("parseFrom", ByteString.class);
                    GeneratedMessage data = (GeneratedMessage) parseFrom.invoke(null, message.getData());
                    listener.onReceive(data);
                } catch (Exception e) {
                    // TODO: handle exception
                }
            } else {
                // no listeners for this type of message
            }
        }

        @Override
        public void onError(Throwable throwable) {
            // TODO: implement onError
        }

        @Override
        public void onCompleted() {
            // TODO: implement onCompleted
        }
    }

    private static class MessageDeliveryStatusObserver implements StreamObserver<MessageDeliveryStatus> {

        @Override
        public void onNext(MessageDeliveryStatus messageDeliveryStatus) {
            // TODO: implement onNext
        }

        @Override
        public void onError(Throwable throwable) {
            // TODO: implement onError
        }

        @Override
        public void onCompleted() {
            // TODO: implement onCompleted
        }
    }
}