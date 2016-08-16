package com.acarus.messaging.websocket;

import com.google.protobuf.GeneratedMessage;
import io.grpc.stub.StreamObserver;
import org.java_websocket.WebSocket;

public class WebSocketStreamObserver<T extends GeneratedMessage> implements StreamObserver<T> {

    private WebSocket conn;

    public WebSocketStreamObserver(WebSocket conn) {
        this.conn = conn;
    }

    @Override
    public void onNext(T t) {
        byte[] data = t.toByteArray();
        conn.send(data);
    }

    @Override
    public void onError(Throwable throwable) {
        // TODO: implement onError
    }

    @Override
    public void onCompleted() {
        if (conn != null) {
            conn.close();
        }
    }
}
