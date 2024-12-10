package com.wikimedia.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class WikiMediaKafkaStreamService
{
    Logger logger = LoggerFactory.getLogger(WikiMediaKafkaStreamService.class.getSimpleName());
    private static final Logger log = LoggerFactory.getLogger(WikiMediaKafkaStreamService.class);
    private final StreamsBuilder steamsBuilder;
    private final  StreamsConfig streamConfig;

    public WikiMediaKafkaStreamService(StreamsBuilder steamsBuilder, StreamsConfig streamConfig)
    {
        this.steamsBuilder =  steamsBuilder;
        this.streamConfig = streamConfig;
    }


    @PostConstruct
    public void startKafkaStream()throws Exception
    {
        String src_topic = "wikimedia.recentchanges";
        String new_topic = "wikimedia.recentchanges.stats";
        //5min for ptoduced time slice
        final long timediff = 300000;

        KStream<String,String> stream = steamsBuilder.stream(src_topic,Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String,String>  timeStream = stream.map((key, value)->{

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node= null;

            try
            {
                node = (ObjectNode)mapper.readTree(value);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            long time = node.get("Produced_Time").asLong();
            String timeSlice = "Produced_5min_agg_"+(time/timediff);


            return KeyValue.pair(key,timeSlice);
        });


        KStream<String,Long> aggstream = timeStream.groupBy((k,v)->v,Grouped.with(Serdes.String(),Serdes.String())).count().toStream();

        aggstream.to(new_topic);

        //convert input-stream key into time based inputstream



//        KTable<Windowed<String>, Long> timebasedWindowStream = newStream.groupByKey(Grouped.with(Serdes.String(), new Serde<JsonNode>() {
//            @Override
//            public Serializer<JsonNode> serializer() {
//                return null;
//            }
//
//            @Override
//            public Deserializer<JsonNode> deserializer() {
//                return null;
//            }
//        })).windowedBy(TimeWindows.of(Duration.ofMinutes(5))).count();

//        KTable<Windowed<String>, Long> timebasedWindowStream = stream.groupByKey(Grouped.with(Serdes.String(),Serdes.String())
//        ).windowedBy(TimeWindows.of(Duration.ofMinutes(5))).count();
//
//
//        timebasedWindowStream.toStream().to(new_topic);


//        KGroupedStream<String, String> groupedStream = stream.groupBy((key,value)->key,Grouped.with(Serdes.String(),Serdes.String()));
//
       //  KTable<String,Long> keyCount = stream.groupByKey().count();
//
     //   keyCount.toStream().to(new_topic);

//       KStream<String, String> processedStream = stream.mapValues(value -> value.toUpperCase());
//        processedStream.to(new_topic,Produced.with(Serdes.String(), Serdes.String()));



       /* KTable<String,Long> keyCount = stream
            .mapValues(
                    textLine -> textLine.toLowerCase())
            .flatMapValues(
                    textLine
                            -> Arrays.asList(
                            textLine.split(",")))
            .selectKey((key, word) -> word)
            .groupByKey()
            .count(Named.as("Counts"));*/


//        keyCount.toStream().foreach((key, value) ->
//               logger.info("Key: " + key + ", Count: " + value)
//
//                stream.to
//        );
//        keyCount.toStream().to(new_topic);



        Topology topology = steamsBuilder.build();

        logger.info("Topology ::"+topology.describe());

        KafkaStreams streams = new KafkaStreams(topology,streamConfig);



        logger.info("Stream:::started");
        streams.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close));

        while (true) {
            System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                break;
            }
        }


    }



}
