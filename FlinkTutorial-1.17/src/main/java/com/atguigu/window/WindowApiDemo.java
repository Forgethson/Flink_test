package com.atguigu.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author cjp
 * @version 1.0
 */
public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 3L, 3),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 3L, 3)
        );
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 指定 窗口分配器： 指定 用 哪一种窗口 ---  时间 or 计数？ 滚动、滑动、会话？
        // 1.1 没有keyby的窗口: 窗口内的 所有数据 进入同一个 子任务，并行度只能为1
//        sensorDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 1.2 有keyby的窗口: 每个key上都定义了一组窗口，各自独立地进行统计计算

        // 基于时间的
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10))); // 滚动窗口，窗口长度10s
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动窗口，窗口长度10s，滑动步长2s
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); // 会话窗口，超时间隔5s
//        sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(5))); // 滚动事件时间窗口，超时间隔5s
//        sensorKS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))); // 滑动事件时间窗口，窗口长度10s，滑动步长2s
//        sensorKS.window(EventTimeSessionWindows.withGap(Time.seconds(5))); // 会话事件时间窗口，超时间隔5s

        // 基于计数的
//        sensorKS.countWindow(5)  // 滚动窗口，窗口长度=5个元素
//        sensorKS.countWindow(5,2) // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
//        sensorKS.window(GlobalWindows.create())  // 全局窗口，计数窗口的底层就是用的这个，需要自定义的时候才会用

        // 2. 指定 窗口函数 ： 窗口内数据的 计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 增量聚合： 来一条数据，计算一条数据，窗口触发的时候输出计算结果
        sensorWS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor ws1, WaterSensor ws2) {
                        System.out.println("调用reduce方法，ws1=" + ws1 + ",ws2=" + ws2);
                        return new WaterSensor(ws1.getId(), ws2.getTs(), ws1.getVc() + ws2.getVc());
                    }
                }
        ).print();

        // 全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        sensorWS.process()

        env.execute();
    }
}
