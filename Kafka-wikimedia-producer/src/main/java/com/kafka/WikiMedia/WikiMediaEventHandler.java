package com.kafka.WikiMedia;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

public class WikiMediaEventHandler implements EventHandler
{
    String topic;
    KafkaProducer<String,String> producer;
    HttpServletResponse res;
    PrintWriter writer;

    public final Logger logger = LoggerFactory.getLogger(WikiMediaEventHandler.class.getSimpleName());


    public WikiMediaEventHandler(KafkaProducer<String,String> producer, String topic, HttpServletResponse res)throws Exception
    {

        logger.info(" WikiBackGroundEventHandler constructor...");

        this.producer = producer;
        this.topic = topic;
        this.res = res;
        writer = this.res.getWriter();
    }

    @Override
    public void onOpen() throws Exception {

        logger.info(" message on open...");

    }

    @Override
    public void onClosed() throws Exception {

        logger.info(" message onclosed...");
        producer.close();
        logger.info(" producer closed...");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception
    {
        long time = System.currentTimeMillis();
        logger.info("producing message...");

        String data = messageEvent.getData();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(data);

        String id = node.get("meta").get("id").asText();


        ((ObjectNode)node).put("Produced_Time",time);

        data = node.toString();

        producer.send(new ProducerRecord<>(topic,id,data));

        logger.info("key="+id+ "::: value="+data);
        writer.println(messageEvent.getData().toString());
    }

    @Override
    public void onComment(String s) throws Exception {

        logger.info("on commit...");

    }

    @Override
    public void onError(Throwable throwable) {

        logger.info(" message error...");
    }
}
