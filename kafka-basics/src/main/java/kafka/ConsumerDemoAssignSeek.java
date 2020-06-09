package kafka;

import com.jefff.udemy.kafka.utility.Utility;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ConsumerDemoAssignSeek {
    private static final Logger _log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);


    // This little utility app is one that just tries to read 'desiredRecords' records at 'desiredOffset'
    // from a particular topic-partition every time it is run
    //
    public static void main(String[] args) {
        Properties properties = Utility.getConsumerProperties(null, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // We want to read a particular topic-partition
        TopicPartition desiredPartition = new TopicPartition("test", 0);
        consumer.assign(Collections.singletonList(desiredPartition));

        // And we want to read starting at offset 5
        int desiredOffset = 3;
        consumer.seek(desiredPartition, desiredOffset);

        boolean done = false;
        int recordsReceived = 0;
        int recordsDesired = 5;

        while (!done) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000L));
            Set<TopicPartition> partitions = records.partitions();
            if (partitions.size() == 0) {
                continue;
            }
            _log.info("Received records from {} partitions", partitions.size());

            for (TopicPartition topicPartition : partitions) {

                List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                _log.info("Received {} records from {}", partitionRecords.size(), topicPartition);

                for (ConsumerRecord record : partitionRecords) {
                    long offset = record.offset();
                    _log.info("Key: {}, Value: {}, Topic: {}, Partition: {}, Offset: {}",
                              record.key(),
                              record.value(),
                              record.topic(),
                              record.partition(),
                              offset);
                    recordsReceived++;
                    if (recordsReceived >= recordsDesired) {
                        done = true;
                        break;
                    }
                }
            }
        }
    }
}
