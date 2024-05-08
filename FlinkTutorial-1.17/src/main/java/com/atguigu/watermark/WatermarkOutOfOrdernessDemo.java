package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.utils.DataSourceUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);

        /*
         * 演示watermark多并行度下的传递
         * 1、接收到上游多个，取最小
         * 2、往下游多个发送， 广播
         */

        // 1. 自定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：乱序的，等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 1.2 指定 时间戳分配器，从数据中提取
                .withTimestampAssigner(
                        (element, recordTimestamp) -> {
                            System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                            // 返回事件时间，要毫秒（如果本身是秒级别，则需要*1000L）
                            return element.getTs();
                        });

        // 2. 指定 watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDSwithWatermark.keyBy(WaterSensor::getId)
                // 3.使用 事件时间语义 的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {

                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = elements.spliterator().estimateSize();

                                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
/**
 * 内置Watermark的生成原理
 * 1、都是周期性生成的： 默认200ms
 * 2、有序流：  watermark = 当前最大的事件时间 - 1ms
 * 3、乱序流：  watermark = 当前最大的事件时间 - 延迟时间 - 1ms
 */