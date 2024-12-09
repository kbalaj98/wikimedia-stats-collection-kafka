package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
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

        String topic="wikimedia.recentchanges";

        try (client;consumer)
        {
            consumer.subscribe(Arrays.asList(topic));
            boolean isIndexExist =  client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            if(!isIndexExist)
            {
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

                    if(!datas.isEmpty())
                    {
                        BulkRequest bulkRequest = new BulkRequest();
                        for(ConsumerRecord<String,String> data:datas)
                        {
                            //To avoid duplicate result
                            String id = OpenSearchUtil.getWikiDataUniqueId(data.value());

                           // logger.info("key:"+data.key()+" value:"+data.value() + " id:"+id);
                            try
                            {
                                IndexRequest indexRequest = new IndexRequest(indexName)
                                        .source(data.value(), XContentType.JSON).id(id);

                                bulkRequest.add(indexRequest);

                              //  IndexResponse response = client.index(indexRequest,RequestOptions.DEFAULT);

                                //logger.info("Indexed resp id="+response.getId());
                            }
                            catch (Exception e)
                            {
                                logger.info(e.toString());
                            }
                        }

                        if(bulkRequest.numberOfActions()>0)
                        {
                            BulkResponse blkresponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);
                            logger.info("Bulk record inserted= "+blkresponse.getItems().length+ " record(s)");

                            consumer.commitSync();
                            logger.info("Offset commited size= "+datas.count());

                        }

                    }

                }
            }
            catch (WakeupException ex)
            {
                logger.info("Consumer is starting to shutdown");
            }
            catch (Exception e)
            {
                logger.info("unexpected exception"+e);
            }
            finally {
                consumer.close();
                client.close();
            }

        }
    }
}
