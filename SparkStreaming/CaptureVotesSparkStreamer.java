package SparkStreaming;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;

import scala.Tuple2;

public class CaptureVotesSparkStreamer {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("CaptureVotesStreamerSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(120000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("votes");

        final JavaInputDStream<ConsumerRecord<String, String>> stream
                = KafkaUtils.createDirectStream(ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        stream.mapToPair(
                new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                        return new Tuple2<>(record.key(), record.value());
                    }
                }).foreachRDD(rdd -> {
                    rdd.values().map(x->x.split(",")).saveAsTextFile("C:\\Users\\Dream\\Documents\\NetBeansProjects\\SparkJava\\src\\SparkStreaming\\testlog1\\");
            //rdd.saveAsTextFile("C:\\Users\\Dream\\Documents\\NetBeansProjects\\SparkJava\\src\\SparkStreaming\\testlog1\\");
        });
        
        
        //stream.foreachRDD(rdd -> {rdd.saveAsTextFile("C:\\Users\\Dream\\Documents\\NetBeansProjects\\SparkJava\\src\\SparkStreaming\\testlog1\\");});
        
       
        ssc.start();
  ssc.awaitTermination();
    }
}
