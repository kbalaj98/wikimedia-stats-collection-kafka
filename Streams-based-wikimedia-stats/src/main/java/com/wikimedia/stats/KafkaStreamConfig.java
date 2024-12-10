package com.wikimedia.stats;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamConfig
{
    @Bean
    public StreamsConfig getKafkaStreamConfig()
    {

        Map<String, String> prop = new HashMap<>();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "Key-wise-count");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
       // prop.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");


        return new StreamsConfig(prop);
    }

    @Bean
    public StreamsBuilder getStreamsBuilder()
    {
        return new StreamsBuilder();
    }

}
