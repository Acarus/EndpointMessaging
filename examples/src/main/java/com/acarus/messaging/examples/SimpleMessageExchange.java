package com.acarus.messaging.examples;

import com.acarus.messaging.MessageClient;
import com.acarus.messaging.MessageClient.MessageListener;
import com.acarus.messaging.gen.TextMessageProto.TextMessage;
import lombok.Cleanup;

import java.util.Scanner;

public class SimpleMessageExchange {

    private static final String MESSAGING_SERVER_HOST = "127.0.0.1";
    private static final int MESSAGING_SERVER_PORT = 9057;

    public static void main(String[] args) throws Exception {
        String host = MESSAGING_SERVER_HOST;
        int port = MESSAGING_SERVER_PORT;
        if (args.length > 1) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        @Cleanup MessageClient client = new MessageClient(host, port);
        System.out.println("Endpoint id: " + client.getEndpointInfo().getId());
        client.addMessageListener(new TextMessageListener(), TextMessage.class);

        Scanner in = new Scanner(System.in);
        while (true) {
            String receiver = in.next();
            TextMessage message = TextMessage.newBuilder()
                    .setText(in.next())
                    .build();
            client.sendMessage(message, receiver);
        }
    }

    private static class TextMessageListener implements MessageListener<TextMessage> {
        public void onReceive(TextMessage textMessage) {
            System.out.printf("Received message: %s\n", textMessage.getText());
        }
    }
}
