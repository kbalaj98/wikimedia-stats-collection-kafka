package com.kafka.WikiMedia;


import com.kafka.WikiMedia.producer.RecentWikiMediaProducerService;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerHomeController
{

    public final Logger logger = LoggerFactory.getLogger(ProducerHomeController.class.getSimpleName());

    @Autowired
    RecentWikiMediaProducerService wikiMediaProducer;

    @PostMapping("wiki-media/producer/start")
    public String StartProducing(HttpServletResponse res)throws Exception
    {

        if(!wikiMediaProducer.isConsume())
        {
            wikiMediaProducer.setRes(res);
            wikiMediaProducer.produce();
        }

        return "Producer  already started";
    }

    @PostMapping("wiki-media/producer/stop")
    public String stopProducing()
    {
        if(wikiMediaProducer.isConsume())
        {
            wikiMediaProducer.setConsume(false);
            return "Producer stopped";
        }

        return "Producer not found";
    }

}
