package com.zhuinden.sparkexperiment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaSample {

    public static void main(String[] args) throws Exception {

//System.setProperty("hadoop.home.dir", "D:\\utils\\spark-util");

        SparkConf conf = new SparkConf().setMaster("spark://jojang-lenovo:7077").setAppName("net.my.kafka.KafkaSample");

        //.set("spark.sql.warehouse.dir", "file:///D:\\utils\\spark-util/spark-warehouse");
        JavaSparkContext sc = new JavaSparkContext(conf);

                SparkSession sso= SparkSession
                .builder()
                .sparkContext(sc.sc())
                .appName("Java Spark SQL basic example")
                .getOrCreate();


        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sso.sparkContext()), Durations.milliseconds(3000));

        Map<String, Object> params = new HashMap<>();
        params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //카프카 consumer서버
        params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        params.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-2");
        params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<String> topics = Arrays.asList("test");

        JavaInputDStream<ConsumerRecord<String, String>> ds = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, params));

        ds.flatMap((ConsumerRecord<String, String> record) -> Arrays.asList(record.value().split(" ")).iterator())
                .mapToPair((String word) -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((Integer v1, Integer v2) -> v1 + v2)
                .print();



        ssc.start();
        ssc.awaitTermination();
    }
}

//spark-submit --jars kafka-clients-1.1.0.jar,spark-streaming_2.11-2.3.0.jar,spark-streaming-kafka-0-10_2.11-2.3.0.jar --class org.apache.spark.examples.streaming.JavaDirectKafkaWordCount target/original-spark-examples_2.11-2.4.0-SNAPSHOT.jar <brokerip:port> <topic>