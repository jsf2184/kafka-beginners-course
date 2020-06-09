package kafka;

import com.jefff.udemy.kafka.utility.Utility;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {
    private static final Logger _log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = Utility.getConsumerProperties("my-group", true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("test"));

        while (true) {

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
                    Map<TopicPartition, OffsetAndMetadata> map
                            = Collections.singletonMap(topicPartition,
                                                       new OffsetAndMetadata(offset + 1));
                    consumer.commitSync(map);
                }
            }
        }
    }

}
