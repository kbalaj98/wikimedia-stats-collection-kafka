package com.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;

@Controller
public class ConsumerHomeController
{
    Logger logger = LoggerFactory.getLogger(ConsumerHomeController.class);
    @Autowired
    OpenSearchConsumer consumer;

    @RequestMapping("/")
    public String Homepage()
    {
        logger.info("consumer home page...... started");
        return "consume";
    }

    @RequestMapping("/startConsume")
    public void startConsume() throws IOException {

        consumer.consume();

    }




}
