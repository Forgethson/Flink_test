package com.atguigu.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9091") // 指定kafka节点的地址和端口
                .setGroupId("GC_wjd_Flink")  // 指定消费者组的id
                .setTopics("test_Flink_02")   // 指定消费的 Topic
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定反序列化器，这里设置了只反序列化value
                .setStartingOffsets(OffsetsInitializer.earliest())  // flink消费kafka的offset重置策略
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource").print("Kafka");
        env.execute();
    }
}
/**
 * kafka消费者的参数：
 * auto.reset.offsets
 * earliest: 如果有offset，从offset继续消费; 如果没有offset，从 最早 消费
 * latest  : 如果有offset，从offset继续消费; 如果没有offset，从 最新 消费
 * <p>
 * flink的kafkasource，offset消费策略：OffsetsInitializer，默认是 earliest
 * earliest: 一定从 最早 消费
 * latest  : 一定从 最新 消费
 */