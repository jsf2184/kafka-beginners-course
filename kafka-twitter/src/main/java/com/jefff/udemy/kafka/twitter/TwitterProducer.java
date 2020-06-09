package com.jefff.udemy.kafka.twitter;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.jefff.udemy.kafka.utility.Utility;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final Logger _log = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        _log.info("Hello Twitter World");
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();

        // create a twitter client

    }

    public TwitterProducer() {
    }

    public void run() {

        // Attempts to establish a connection.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        KafkaProducer<String, String> kafkaProducer = Utility.createKafkaProducer();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            _log.info("Runtime shutdown hook triggered. Call consumerRunnable.shutdown()");
            twitterClient.stop();
            Utility.sleep(3000);
        }));


// on a different thread, or multiple different threads....
        int count = 0;
        Gson gson = new Gson();

        while (!twitterClient.isDone()) {
            String msg = readMessage(msgQueue);
            if (msg == null) continue;

            Tweet tweet = gson.fromJson(msg, Tweet.class);
            _log.info("{}, id_str: {},  text: {}", count, tweet.id_str, tweet.text);
            count++;

            ProducerRecord<String, String> producerRecord
                    = new ProducerRecord<>("twitter_tweets", null, msg);

            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                if (e != null) {
                    _log.error("Caught exception sending to kafka: {}", e.getMessage());
                }
            });
        }
        _log.info("Completed runLoop in main thread, close producer");
        kafkaProducer.close();
        _log.info("KafkaProducer closed");

    }

    public static String readMessage(BlockingQueue<String> msgQueue) {
        String msg = null;
        try {
            msg = msgQueue.poll(3000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            _log.error("msgQueue.poll {}", e.getMessage());
        }
        return msg;

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("Trump");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file

        Authentication hosebirdAuth = TwitterUtility.createAuthentication();

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        ClientBuilder builder
                = new ClientBuilder().name("Hosebird-Client-01")                              // optional: mainly for the logs
                                     .hosts(hosebirdHosts)
                                     .authentication(hosebirdAuth)
                                     .endpoint(hosebirdEndpoint)
                                     .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

}
