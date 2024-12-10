package com.kafka.WikiMedia.producer;

import com.kafka.WikiMedia.KafkaProducerUtil;
import com.kafka.WikiMedia.WikiMediaEventHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;


@Service
public class RecentWikiMediaProducerService
{

    HttpServletResponse res;

    private volatile boolean isConsume = false;

    //setter injection
    public void setRes(HttpServletResponse res)
    {
        this.res = res;
    }

    public synchronized void produce() throws Exception
    {

        setConsume(true);

        KafkaProducer<String,String> producer = KafkaProducerUtil.getProducer();

        String topic = "wikimedia.recentchanges";


        EventHandler handler = new WikiMediaEventHandler(producer,topic,res);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(handler,URI.create(url));

        //start producer
        try( EventSource source = builder.build();)
        {
            source.start();

            while(isConsume())
            {
                //main thread stop
                TimeUnit.SECONDS.sleep(20);
            }
        }
    }


    public boolean isConsume()
    {
        return isConsume;
    }

    public void setConsume(boolean consume)
    {
        isConsume = consume;
    }

}
