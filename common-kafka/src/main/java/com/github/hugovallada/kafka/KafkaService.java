package com.github.hugovallada.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupdId, String topic, ConsumerFunction parse, Class<T> type, Map<String,String> properties) {
        this(parse, groupdId, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupdId, Pattern topic, ConsumerFunction parse, Class<T> type,Map<String,String> properties) {
        this(parse,groupdId,type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupdId, Class<T> type,Map<String,String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupdId, type, properties));
    }



    void run(){
        while(true){
            var records = consumer.poll(Duration.ofMillis(5000));
            if(!records.isEmpty()){
                System.out.println("Encontrei " + records.count()+ " registros");
                records.forEach(parse::consume);
            }
        }
    }

    private Properties getProperties(String groupdId, Class<T> type, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupdId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
