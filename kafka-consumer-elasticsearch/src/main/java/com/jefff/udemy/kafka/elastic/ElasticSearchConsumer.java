package com.jefff.udemy.kafka.elastic;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jefff.udemy.kafka.utility.Utility;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final Logger _log = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static final String HOST_NAME = "jsf-play-9449320554.us-east-1.bonsaisearch.net";
    private static final String USER_NAME = "aig3k1rvdq"; // bonsai accessKey
    private static final String PASSWORD = "fc6uj3v020"; // bonsai accessSecret
    private static final int PORT = 443;
    private static final String SCHEME = "https";

    private static final String TOPIC = "twitter_tweets";

    public static void main(String[] args) {
        _log.info("Hello elasticsearch");

        boolean manualCommit = false;
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(manualCommit);
        RestHighLevelClient elasticClient = createElasticClient();

        while (true) {

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000L));
            int count = records.count();
            if (count == 0) {
                _log.info("Nothing to process for now");
                continue;
            }
            try {
                _log.info("Begin processing of {} records", count);
                processBatch(records, elasticClient);
                _log.info("Successful processing of {} records", count);
                kafkaConsumer.commitSync();
            } catch (IOException e) {
                _log.error("Caught exception: '{}' processing batch of {} records",
                           e.getMessage(),
                           count);
            }
        }
//        elasticClient.close();
    }

    public static void processBatch(ConsumerRecords<String, String> records, RestHighLevelClient client)
            throws IOException {

        BulkRequest bulkRequest = new BulkRequest();
        int bulkSize = 0;
        for (ConsumerRecord<String, String> record : records) {

            String kafkaId = buildKafkaId(record);
            String jsonTweet = record.value();
            String twitterId = extractTwitterId(jsonTweet);

            if (Utility.isEmpty(twitterId)) {
                _log.warn("No twitterId in message: {}", jsonTweet);
                continue;
            }

            IndexRequest indexRequest = new IndexRequest("twitter");
            indexRequest
                    .source(jsonTweet, XContentType.JSON)
                    .id(twitterId);


            bulkRequest.add(indexRequest);
            _log.info("Added kafkaId: {}, twitterId: {} as record {} to bulkRequest ",
                      kafkaId, twitterId, bulkSize);
            bulkSize++;

//            IndexResponse indexResponse;
//            try {
//                 indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//            } catch (ElasticsearchException e) {
//                _log.error("client.index threw exception: {} - {}", e.getClass().getSimpleName(), e.getMessage());
//                _log.error(jsonTweet);
//                continue;
//            }
//            String esId = indexResponse.getId();
//            Utility.sleep(100);
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        int responseLength = bulkResponse.getItems().length;
        _log.info("processBatch(): batchSize={}, responseLength={}, hasErrors={}",
                  bulkSize,
                  responseLength,
                  bulkResponse.hasFailures());
    }

    public static String extractTwitterId(String jsonTweet) {
        JsonElement jsonElement = JsonParser.parseString(jsonTweet);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement id_strElement = jsonObject.get("id_str");
        if (id_strElement == null) {
            return null;
        }
        String twitterId = id_strElement.getAsString();
        return twitterId;
    }

    public static String buildKafkaId(ConsumerRecord<String, String> record) {
        String result = record.topic() + "_" + record.partition() + "_" + record.offset();
        return result;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(boolean manualCommit) {
        Properties consumerProperties = Utility.getConsumerProperties("my-group", manualCommit);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public static RestHighLevelClient createElasticClient() {

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(USER_NAME, PASSWORD);
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        RestClientBuilder builder = RestClient.builder(new HttpHost(HOST_NAME, PORT, SCHEME));
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            _log.info("provide credentialProvider in clientConfigCallback");
            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpAsyncClientBuilder;
        });
        RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);
        return highLevelClient;
    }
}
