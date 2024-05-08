package com.atguigu.watermark;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.utils.DataSourceUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
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
 * 自定义水位线生成策略
 *
 * @author cjp
 * @version 1.0
 */
public class WatermarkCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);

        // 默认周期 200ms
        env.getConfig().setAutoWatermarkInterval(2000);

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.自定义的 周期性生成
                .<WaterSensor>forGenerator(ctx -> new MyPeriodWatermarkGenerator<>(3000L))
                // 2.自定义的 断点式生成
//                .<WaterSensor>forGenerator(ctx -> new MyPuntuatedWatermarkGenerator<>(3000L))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs());

        SingleOutputStreamOperator<WaterSensor> sensorDSwithWatermark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);


        sensorDSwithWatermark.keyBy(WaterSensor::getId)
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
