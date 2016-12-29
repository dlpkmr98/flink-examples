/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkaflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *
 * @author dlpkmr98
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        DataSet<String> text = env.readTextFile("C:\\Users\\dlpkmr98\\Desktop\\FACT_03_QTR_P.SQL");
        DataSet<Tuple2<String, Integer>> counts
                = // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // normalize and split the line
                        String[] tokens = value.toLowerCase().split("\\W+");
                        // emit the pairs
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                out.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }

                }).groupBy(0).sum(1);
       
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();

    }
}
