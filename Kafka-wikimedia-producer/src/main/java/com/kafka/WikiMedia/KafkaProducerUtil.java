package com.kafka.WikiMedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerUtil
{

    public static KafkaProducer<String,String> getProducer()
    {
        String server = "127.0.0.1:9092";

        Properties prop = new Properties();

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //high throughput
//        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
//        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
//        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        return new KafkaProducer<String,String>(prop);
    }
}
