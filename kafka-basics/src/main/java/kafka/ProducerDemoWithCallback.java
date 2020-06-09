package kafka;

import com.jefff.udemy.kafka.utility.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerDemoWithCallback {

    private static final Logger _log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static class SendRec {
        ProducerRecord<String, String> _producerRecord;
        Future<RecordMetadata> _future;

        public SendRec(ProducerRecord<String, String> producerRecord, Future<RecordMetadata> future) {
            _producerRecord = producerRecord;
            _future = future;
        }

        public ProducerRecord<String, String> getProducerRecord() {
            return _producerRecord;
        }

        public Future<RecordMetadata> getFuture() {
            return _future;
        }

        public void checkOutcome() {
            try {
                RecordMetadata metadata = _future.get();
                _log.info("SendRec.checkOutcome(): From future.get(): for send with key; {}, msg: {}, outcome={}",
                          _producerRecord.key(),
                          _producerRecord.value(),
                          Utility.toString(metadata));

            } catch (InterruptedException e) {
                _log.error("SendRec.checkOutcome(): InterruptedException: for send with key; {}, msg: {} outcome = {}",
                           _producerRecord.key(),
                           _producerRecord.value(),
                           e.getMessage());

            } catch (ExecutionException e) {
                _log.error("SendRec.checkOutcome(): ExecutionException: for send with key; {}, msg: {} outcome = {}",
                           _producerRecord.key(),
                           _producerRecord.value(),
                           e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        _log.info("Hello world {}", "jeff");

        // create the producer
        KafkaProducer<String, String> producer = Utility.createKafkaProducer();

        List<Future<RecordMetadata>> set = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String key = Integer.toString(i % 10);
            String message = "HI " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", key, message);
//            System.out.println("about to send");
            Future<RecordMetadata> future = producer.send(producerRecord, ProducerDemoWithCallback::onCompletion);
            SendRec sendRec = new SendRec(producerRecord, future);
            sendRec.checkOutcome();
        }
        _log.info("I collected {} futures", set.size());
        Duration duration = Duration.ofSeconds(60);
        producer.close();
        System.out.println("about to exit");
    }

    // Note, in my mind, the RecordMethadata class is sorely lacking in the info it should have to
    // correlate the callback back to the record that it is reporting on.
    //
    static void onCompletion(RecordMetadata metadata, Exception e) {
        if (e == null) {
            _log.info("onCompletion: {}", Utility.toString(metadata));
        } else {
            _log.error("onCompletion: Caught exception: {} {}", e.getClass().getSimpleName(), e.getMessage());
        }

    }


}
