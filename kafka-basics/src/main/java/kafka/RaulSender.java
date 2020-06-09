package kafka;// I posted questions to the UDEMY class about the lack of good information (in particular the sent record)
// that is passed back to the kafka callback. A response was provided about this capability being
// available in Springboot. Here is the guy's solution.


//package com.jefff.udemy.kafka;
//
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.springframework.kafka.support;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//
//public class RaulSender {
//
//    public static class Person {
//    }
//
//    public void sendThem( List<Person> toSend) throws InterruptedException {
//        List<ListenableFuture<SendResult<String, Person>>> futures = new ArrayList<>();
//        CountDownLatch latch = new CountDownLatch(toSend.size());
//        ListenableFutureCallback<SendResult<String, Person>> callback = new ListenableFutureCallback<SendResult<String, Person>>() {
//
//            @Override
//            public void onSuccess(SendResult<String, Person> result) {
//                LOG.info(result.getRecordMetadata().toString());
//                latch.countDown();
//            }
//
//            @Override
//            public void onFailure(Throwable ex) {
//                ProducerRecord<?, ?> producerRecord = ((KafkaProducerException) ex).getProducerRecord();
//                LOG.error("Failed; " + producerRecord.value(), ex);
//                latch.countDown();
//            }
//        };
//        toSend.forEach(str -> {
//            ListenableFuture<SendResult<String, Person>> future = template.send("t_101", str);
//            future.addCallback(callback);
//        });
//}
