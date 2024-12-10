package com.wikimedia.stats;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsRESTController
{
    Logger logger = LoggerFactory.getLogger(StatsRESTController.class.getSimpleName());

    @Autowired
    WikiMediaKafkaStreamService service;


    @PostMapping("/stats/topicSpecificAllStats")
    public String getAllRecentTopicStats()throws Exception
    {
        logger.info("calling startKafkaStream");

        service.startKafkaStream();

        logger.info("completed startKafkaStream");


        return "kafka stream produced";

    }

}
