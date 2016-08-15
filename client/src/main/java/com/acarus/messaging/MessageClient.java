package com.acarus.messaging;

import com.acarus.messaging.gen.MessageProviderGrpc;
import com.acarus.messaging.gen.MessageProviderGrpc.MessageProviderStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.acarus.messaging.gen.MessagingProto.*;

public class MessageClient {

    private static final Logger LOG = LoggerFactory.getLogger(MessageClient.class);

    private final ManagedChannel channel;
    private final StreamObserver<Message> outgoingMessageStream;
    private final EndpointInfo endpointInfo;
    private ConcurrentHashMap<String, MessageListener> listeners = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Class> classes = new ConcurrentHashMap<>();

    public MessageClient(String host, int port) {
        LOG.info("Connecting to {}:{}", host, port);
        endpointInfo = EndpointInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .build();

        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build();

        MessageProviderStub asyncStub = MessageProviderGrpc.newStub(channel);
        outgoingMessageStream = asyncStub.sendMessages(new MessageDeliveryStatusObserver());
        asyncStub.receiveMessages(endpointInfo, new MessageObserver());
        LOG.debug("Endpoint: [{}] is started", endpointInfo);
    }

    public EndpointInfo getEndpointInfo() {
        return endpointInfo;
    }

    public <T extends GeneratedMessage> void sendMessage(T msg, String receiver) {
        LOG.debug("Sending message: [{}] to endpoint: {}", msg, receiver);
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
        LOG.debug("Set listener for messageClass: {}", clazz.getName());
    }

    public void close() throws InterruptedException {
        LOG.info("Stopping client");
        if (outgoingMessageStream != null) {
            outgoingMessageStream.onCompleted();
        }

        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        LOG.info("Client is stopped");
    }

    public interface MessageListener<T extends GeneratedMessage> {
        void onReceive(T message, String sender);
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

    private class MessageObserver implements StreamObserver<Message> {

        @Override
        @SuppressWarnings("unchecked")
        public void onNext(Message message) {
            String messageClassName = message.getDataClass();
            if (listeners.containsKey(messageClassName)) {
                Class clazz = classes.get(messageClassName);
                MessageListener listener = listeners.get(messageClassName);
                try {
                    Method parseFrom = clazz.getDeclaredMethod("parseFrom", ByteString.class);
                    GeneratedMessage data = (GeneratedMessage) parseFrom.invoke(null, message.getData());
                    LOG.debug("Received message: [{}] from endpoint: {}", data, message.getSender());
                    listener.onReceive(data, message.getSender());
                } catch (Exception e) {
                    // TODO: handle exception
                }
            } else {
                LOG.warn("Received message but no listeners found");
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
}