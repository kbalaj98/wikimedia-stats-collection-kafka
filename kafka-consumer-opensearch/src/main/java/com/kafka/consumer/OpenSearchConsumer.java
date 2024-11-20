package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;

@Component
public class OpenSearchConsumer
{
    Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);

    @Autowired
    OpenSearchUtil openSearchUtil;

    public void consume() throws IOException
    {
        //openSearch client
        RestHighLevelClient client = openSearchUtil.getRestHighLevelClient();

        //kafka consumer
        KafkaConsumer<String,String> consumer = openSearchUtil.getKafkaConsumer();

        String indexName = "wiki-media";
       // indexName = "wiki-media-2";

        String topic="wikimedia.recentchanges";

        try (client;consumer)
        {
            consumer.subscribe(Arrays.asList(topic));
            boolean isIndexExist =  client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);


            if(!isIndexExist)
            {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("OpenSearch "+indexName+" index");
            }
            else
            {
                logger.info("OpenSearch wikimedia index already exist");
            }

            //main thread ref
            Thread mainThread = Thread.currentThread();

            //adding shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run()
                {
                    logger.info("Detect main thread shutdown,exit by calling consumer wakeup");
                    consumer.wakeup();


                    //join the main thread allow to execute the code in main thread
                    try
                    {
                        mainThread.join();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });


            try
            {
                while(true)
                {
                    //  logger.info("Starting polling");
                    ConsumerRecords<String,String> datas = consumer.poll(Duration.ofMillis(1000));

                    if(datas.isEmpty())
                    {
                        logger.info("empty consume...");
                    }

                    for(ConsumerRecord<String,String> data:datas)
                    {
                        logger.info("key:"+data.key()+" value:"+data.value());
                        logger.info("partition:"+data.partition()+" Offset:"+data.offset());
                    }

                }
            }
            catch (WakeupException ex)
            {
                logger.info("Consumer is starting to shutdown");
            }

        }
    }
}
