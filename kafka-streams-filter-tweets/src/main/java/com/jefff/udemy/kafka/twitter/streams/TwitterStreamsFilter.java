package com.jefff.udemy.kafka.twitter.streams;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jefff.udemy.kafka.utility.Utility;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TwitterStreamsFilter {

    private static final Logger _log = LoggerFactory.getLogger(TwitterStreamsFilter.class);

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Utility.BOOTSTRAP_SERVERS);

        // This is much like a consumer group id for streams
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");

        // How we will serialize String keys and values
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> filteredStream = streamsBuilder.stream("twitter_tweets");

        filteredStream
                .filter((k, jsonStr) -> extractFollowerCount(jsonStr) > 100)
                .to("important-tweets");


        // build a topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);


        // start our streams application
        kafkaStreams.start();


    }

    public static int extractFollowerCount(String jsonTweet) {
        JsonElement jsonElement = JsonParser.parseString(jsonTweet);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement userElement = jsonObject.get("user");
        if (userElement == null) {
            return 0;
        }
        JsonObject userObject = userElement.getAsJsonObject();
        JsonElement followersCountElement = userObject.get("followers_count");
        if (followersCountElement == null) {
            return 0;
        }
        int result = followersCountElement.getAsInt();
        _log.info("extractFollowerCount:() returning {} followers", result);
        return result;
    }

}
