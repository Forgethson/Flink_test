package com.atguigu.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区
 *
 * @author cjp
 * @version 1.0
 */
public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        lineDS.partitionCustom(
                // 自定义分区器对象
                new MyPartitioner(),
                // 指定分区key的选择规则
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String s) {
                        return s;
                    }
                }).print();
        env.execute();
    }
}
