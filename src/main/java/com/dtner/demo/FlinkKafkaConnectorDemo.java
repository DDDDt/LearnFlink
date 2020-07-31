package com.dtner.demo;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.File;
import java.util.Properties;

/**
 * @ClassName FLinkKafkaConnectorDemo
 * @Description: flink 连接 kafka
 * @Author dt
 * @Date 2020-07-31
 **/
public class FlinkKafkaConnectorDemo {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("group.id", "test_dt");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>("traffic_flow", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> stream = env.addSource(kafkaConsumer);

        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(Path.fromLocalFile(new File("/opt/kafka/")), new SimpleStringEncoder<String>("UTF-8"))
                .build();
        stream.addSink(fileSink);
        env.execute("flink kafka print");

    }

}
