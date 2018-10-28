/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kafka;

import java.security.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author Dream
 */
public class SparkKafkaStreaming {

    static int MAX_USERS = 1000;
    static int MAX_PLAYER = 5;
    Map< Integer, Timestamp> hm = new HashMap<>();

    public static void main(String[] args) {

        //runProducer(1, 5);
        //runConsumer();
    }
    public static Producer<Long, String> producer1=ProducerCreator.createProducer();

    static void runProducer(String value) {
        //Producer<Long, String> producer = ProducerCreator.createProducer();

            //int randUser = ThreadLocalRandom.current().nextInt(1, MAX_USERS);
        //int randVote= ThreadLocalRandom.current().nextInt(1, MAX_PLAYER);
        ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConstants.TOPIC_NAME, value);
        try {
            RecordMetadata metadata = producer1.send(record).get();
            System.out.println("Message Sent");
             } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }

    }

}
