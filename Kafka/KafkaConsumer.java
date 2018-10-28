/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 *
 * @author Dream
 */
public class KafkaConsumer {
    
    public static void main(String[] args) {
        //runProducer();
        runConsumer();
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) // If no message found count is reached to threshold exit loop.  
                {
                    break;
                } else {
                    continue;
                }
            }
            //print each record. 
            consumerRecords.forEach(record -> {
                System.out.println("Record Value : "+record.value()+" Record TimeStamp :"+record.timestamp());
            });
            // commits the offset of record to broker. 
            consumer.commitAsync();
        }
        consumer.close();
    }

}
