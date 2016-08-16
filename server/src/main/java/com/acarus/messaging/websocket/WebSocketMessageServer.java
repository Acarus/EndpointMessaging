package com.acarus.messaging.websocket;

import com.acarus.messaging.MessageService;
import com.acarus.messaging.gen.MessagingProto.EndpointInfo;
import com.acarus.messaging.gen.MessagingProto.Message;
import com.acarus.messaging.gen.MessagingProto.MessageDeliveryStatus;
import com.acarus.messaging.websocket.exceptions.InvalidMessageException;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WebSocketMessageServer extends WebSocketServer {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketMessageServer.class);

    private static final String RECEIVE_MESSAGES = "/receive";
    private static final String SEND_MESSAGES = "/send";
    private static final Set<String> SUPPORTED_OPERATIONS;

    static {
        SUPPORTED_OPERATIONS = new HashSet<>();
        SUPPORTED_OPERATIONS.add(RECEIVE_MESSAGES);
        SUPPORTED_OPERATIONS.add(SEND_MESSAGES);
    }

    private ConcurrentMap<WebSocket, StreamObserver<Message>> inputStreamObservers = new ConcurrentHashMap<>();
    private ConcurrentMap<WebSocket, String> connToUri = new ConcurrentHashMap<>();
    private MessageService messageService;

    public WebSocketMessageServer(int port, MessageService messageService) throws IOException {
        super(new InetSocketAddress(port));
        this.messageService = messageService;
    }

    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
        String Uri = clientHandshake.getResourceDescriptor();
        if (!SUPPORTED_OPERATIONS.contains(Uri)) {
            LOG.error("Unsupported method called");
            webSocket.close();
            return;
        }

        if (Uri.equals(SEND_MESSAGES)) {
            StreamObserver<MessageDeliveryStatus> outputStream = new WebSocketStreamObserver<>(webSocket);
            StreamObserver<Message> inputStream = messageService.sendMessages(outputStream);
            inputStreamObservers.put(webSocket, inputStream);
        }
        connToUri.put(webSocket, Uri);
    }

    @Override
    public void onClose(WebSocket webSocket, int i, String s, boolean b) {
        StreamObserver<Message> observer = inputStreamObservers.get(webSocket);
        observer.onCompleted();
    }

    @Override
    public void onMessage(WebSocket webSocket, String s) {
        // We use only binary messages
        LOG.error("Received text message though websocket connection");
        closeConnection(webSocket, new InvalidMessageException());
    }

    @Override
    public void onMessage(WebSocket webSocket, ByteBuffer message) {
        String URI = connToUri.get(webSocket);
        byte[] data = message.array();
        try {
            if (URI.equals(SEND_MESSAGES)) {
                Message msg = Message.parseFrom(data);
                StreamObserver<Message> observer = inputStreamObservers.get(webSocket);
                observer.onNext(msg);
            } else if (URI.equals(RECEIVE_MESSAGES)) {
                EndpointInfo endpointInfo = EndpointInfo.parseFrom(data);
                StreamObserver<Message> outputStream = new WebSocketStreamObserver<>(webSocket);
                messageService.receiveMessages(endpointInfo, outputStream);
            } else {
                //
            }
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Can not parse received message");
            closeConnection(webSocket, new InvalidMessageException());
            return;
        }
    }

    private void closeConnection(WebSocket conn, Throwable reason) {
        // TODO: implement closeConnection
    }

    @Override
    public void onError(WebSocket webSocket, Exception e) {
        // TODO: implement onError
    }

    public static void main(String[] args) throws IOException {
        WebSocketMessageServer webSocketMessageServer = new WebSocketMessageServer(8887, new MessageService(null, null));
        webSocketMessageServer.start();
        Scanner in = new Scanner(System.in);
        in.nextLine();
    }
}
