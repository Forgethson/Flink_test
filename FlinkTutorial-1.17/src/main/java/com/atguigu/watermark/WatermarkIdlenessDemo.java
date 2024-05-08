package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.partition.MyPartitioner;
import com.atguigu.utils.DataSourceUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WatermarkIdlenessDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        conf.set(RestOptions.BIND_ADDRESS, "localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<Integer> numDS = DataSourceUtil.getNumberDataStreamSource(env);

        // 自定义分区器：数据%分区数，只输入奇数，都只会去往map其中一个子任务
        SingleOutputStreamOperator<Integer> socketDS = numDS.partitionCustom(new MyPartitioner(), Object::toString)
                .map(r -> r)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> System.currentTimeMillis())
                                .withIdleness(Duration.ofSeconds(5))  //空闲等待5s
                );

        // 分成两组： 奇数一组，偶数一组 ， 开10s的事件时间滚动窗口
        socketDS.keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context ctx, Iterable<Integer> elements, Collector<String> out) {
                        String windowStart = DateFormatUtils.format(ctx.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(ctx.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements);
                    }
                })
                .print();
        env.execute();
    }
}
