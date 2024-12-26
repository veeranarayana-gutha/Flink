package com.example.Flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class FlinkApp {
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("Hello");
		
		StreamExecutionEnvironment env
		  = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// Create a DataStream that contains some numbers
//        DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);
//
//        // Apply Flink operators
//        DataStream<Integer> resultDataStream = dataStream
//                .filter(new OddFilter());
//
//        // Print the DataStream
//        resultDataStream.print();
//        
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Sink data
        //dataStream.addSink(...);
        
        KafkaSource<String> source = KafkaSource.<String>builder()
        	    .setBootstrapServers("localhost:9092")
        	    .setTopics("my-topic")
        	    .setGroupId("my-group")
        	    .setStartingOffsets(OffsetsInitializer.earliest())
        	    .setValueOnlyDeserializer(new SimpleStringSchema())
        	    .build();

        DataStream<String> data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        data.print();
        // Execute program
        env.execute("Flink Streaming Job (Filter example)");
	}
	
	private static class OddFilter implements FilterFunction<Integer> {
        @Override
        public boolean filter(Integer value) {
            return value % 2 == 0;
        }
    }

}

