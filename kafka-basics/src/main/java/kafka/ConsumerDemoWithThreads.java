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
import java.util.concurrent.CountDownLatch;

import static com.jefff.udemy.kafka.utility.Utility.sleep;

public class ConsumerDemoWithThreads {
    private static final Logger _log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) {
        Properties properties = Utility.getConsumerProperties("my-group", true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("test"));

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch, consumer);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            _log.info("Runtime shutdown hook triggered. Call consumerRunnable.shutdown()");
            consumerRunnable.shutdown();
            sleep(3000);
        }));

        // Launch a thread that will call System Exit after waiting 10 secodns in that thread.
//        new Thread(()-> {
//            sleep(10000);
//            _log.info("Call system exit");
//            System.exit(0);
//        }).start();

        try {
            _log.info("About to call latch.await()");
            latch.await();
            _log.info("Back from latch.await()");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            _log.info("Application ending");
        }


    }

    public static class ConsumerRunnable implements Runnable {

        CountDownLatch _countDownLatch;
        private KafkaConsumer<String, String> _consumer;

        public ConsumerRunnable(CountDownLatch countDownLatch,
                                KafkaConsumer<String, String> consumer) {
            _countDownLatch = countDownLatch;
            _consumer = consumer;
        }

        @Override
        public void run() {

            while (true) {

                ConsumerRecords<String, String> records = null;
                try {
                    records = _consumer.poll(Duration.ofMillis(3000L));
                } catch (Exception ignore) {
                    _log.info("Received WakeupException in while loop");
                    break;
                }

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
                        _consumer.commitSync(map);
                    }
                }
            }
            _log.info("Finished while loop. Call consumer.close()");
            _consumer.close();
            _log.info("Call latch.countdown()");
            _countDownLatch.countDown();

        }

        public void shutdown() {
            _log.info("ConsumerRunnable.shutdown() invoked");
            // Will cause consumer.poll() to throw a WakeUpException.
            _consumer.wakeup();
        }
    }
}
