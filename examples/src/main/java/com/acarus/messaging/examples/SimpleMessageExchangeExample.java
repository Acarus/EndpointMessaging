package com.acarus.messaging.examples;

import com.acarus.messaging.MessageClient;
import com.acarus.messaging.MessageClient.MessageListener;
import com.acarus.messaging.gen.TextMessageProto.TextMessage;

import java.util.Scanner;

public class SimpleMessageExchangeExample {

    private static final String MESSAGING_SERVER_HOST = "127.0.0.1";
    private static final int MESSAGING_SERVER_PORT = 9057;

    public static void main(String[] args) throws Exception {
        String host = MESSAGING_SERVER_HOST;
        int port = MESSAGING_SERVER_PORT;
        if (args.length > 1) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        MessageClient client = new MessageClient(host, port);
        client.addMessageListener(new TextMessageListener(), TextMessage.class);

        System.out.println("Endpoint id: " + client.getEndpointInfo().getId());
        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.println("Receiver id:");
            String receiver = in.next();
            in.nextLine();
            System.out.println("Text:");
            client.sendMessage(textMessage(in.nextLine()), receiver);
        }
    }

    private static TextMessage textMessage(String text) {
        return TextMessage.newBuilder()
                .setText(text)
                .build();
    }

    private static class TextMessageListener implements MessageListener<TextMessage> {
        public void onReceive(TextMessage textMessage, String sender) {
            System.out.println("Received message: " + textMessage.getText());
            System.out.println("Message sender: " + sender);
        }
    }
}
