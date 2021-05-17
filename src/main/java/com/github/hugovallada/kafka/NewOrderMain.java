package com.github.hugovallada.kafka;


import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        try(var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {

                var key = UUID.randomUUID().toString();

                var value = "132123,67523,1234";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Welcome we are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }


}
