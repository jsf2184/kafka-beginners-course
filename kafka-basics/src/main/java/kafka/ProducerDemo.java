package kafka;

import com.jefff.udemy.kafka.utility.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Hello world");
        // create the producer
        KafkaProducer<String, String> producer = Utility.createKafkaProducer();

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("rep2", "102");
        System.out.println("about to send");
        producer.send(producerRecord);
        System.out.println("completed send");
        producer.close();
        System.out.println("about to exit");
    }
}
