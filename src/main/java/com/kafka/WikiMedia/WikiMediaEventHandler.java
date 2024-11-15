package com.kafka.WikiMedia;


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
        logger.info("producing message...");
        producer.send(new ProducerRecord<>(topic,messageEvent.toString()));

        logger.info(messageEvent.toString());
        writer.println(messageEvent.toString());
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
