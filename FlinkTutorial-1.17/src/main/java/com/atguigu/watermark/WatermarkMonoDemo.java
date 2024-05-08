package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.utils.DataSourceUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WatermarkMonoDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);

        // 1. 自定义Watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：升序的watermark，没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                // 1.2 指定 时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        // 返回事件时间，要毫秒（如果本身是秒级别，则需要*1000L）
                        return element.getTs();
                    }
                });

        // 2. 指定watermark策略
        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);
        sensorDSwithWatermark.keyBy(WaterSensor::getId)
                // 3.使用 事件时间语义 的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                             @Override
                             public void process(String s, Context ctx, Iterable<WaterSensor> elements, Collector<String> out) {
                                 String windowStart = DateFormatUtils.format(ctx.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                                 String windowEnd = DateFormatUtils.format(ctx.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                                 long count = elements.spliterator().estimateSize();
                                 out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements);
                             }
                         }
                )
                .print();
        env.execute();
    }
}
