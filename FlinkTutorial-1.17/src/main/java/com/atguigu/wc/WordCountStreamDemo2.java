package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * TODO DataStream实现Wordcount：读文件（有界流）
 *
 * @author cjp
 * @version 1.0
 */
public class WordCountStreamDemo2 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读取数据:从文件读
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        // 3.处理数据: 切分、转换、分组、聚合
        // 3.1 切分
        SingleOutputStreamOperator<String> words = lineDS.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        // 一行数据按照 空格 切分
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                });
        // 3.15 转化为2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = words.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) {
                        return Tuple2.of(value, 1);
                    }
                });
        // 3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        // 3.3 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);
        // 4.输出数据
        sumDS.print();
        // 5.执行：类似 sparkstreaming最后 ssc.start()
        env.execute();
    }
}

/**
 * 接口 A，里面有一个方法a()
 * 1、正常实现接口步骤：
 * <p>
 * 1.1 定义一个class B  实现 接口A、方法a()
 * 1.2 创建B的对象：   B b = new B()
 * <p>
 * <p>
 * 2、接口的匿名实现类：
 * new A(){
 * a(){
 * <p>
 * }
 * }
 */