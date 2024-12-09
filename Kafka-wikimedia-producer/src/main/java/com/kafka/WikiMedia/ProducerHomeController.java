package com.kafka.WikiMedia;


import com.kafka.WikiMedia.producer.RecentWikiMediaProducer;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HomeController
{

    public final Logger logger = LoggerFactory.getLogger(HomeController.class.getSimpleName());

    @Autowired
    RecentWikiMediaProducer wikiMediaProducer;

    @RequestMapping("/")
    public String home()
    {
        logger.info("Home page...");

        return "index";
    }

    @RequestMapping("/startProduce")
    public void StartProducing(HttpServletResponse res)throws Exception
    {

//        RecentWikiMediaProducer wikiMediaProducer = new RecentWikiMediaProducer(res);

        wikiMediaProducer.setRes(res);
        wikiMediaProducer.produce(2);
    }

}
