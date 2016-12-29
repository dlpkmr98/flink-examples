/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaflink;

import java.util.Properties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 *
 * @author dlpkmr98
 */
public class ReadFromKafka {
    
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // parse user parameters
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer08<>("filedata", new SimpleStringSchema(), properties));

        // print() will write the contents of the stream to the TaskManager's standard out stream
        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        messageStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentences, Collector<Tuple2<String, Integer>> clctr) throws Exception {
                for (String words : sentences.split(",")) {
                    clctr.collect(new Tuple2<String, Integer>(words, 1));
                }
                
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(1))
                .sum(1).print();

//writeAsText("C:\\Users\\dlpkmr98\\Desktop\\kafkadata\\output.txt");
        env.execute();
    }
    
}
