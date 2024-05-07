package com.atguigu.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.utils.DataSourceUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(WaterSensor::getId);

        // 1. 窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2. 窗口函数： 增量聚合 Aggregate
        /*
         * 1、属于本窗口的第一条数据来，创建窗口，创建累加器
         * 2、增量聚合： 来一条计算一条， 调用一次add方法
         * 3、窗口输出时调用一次getResult方法
         * 4、输入、中间累加器、输出 类型可以不一样，非常灵活
         */
        // 验证结果是否和调用sum一致
        // sensorWS.sum("vc").print();
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                /*
                 * 第一个类型： 输入数据的类型
                 * 第二个类型： 累加器的类型，存储的中间计算结果的类型
                 * 第三个类型： 输出的类型
                 */
                new AggregateFunction<WaterSensor, Tuple2<String, Integer>, String>() {
                    // 创建累加器，初始化累加器
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
//                        System.out.println("创建累加器");
                        return new Tuple2<>("", 0);
                    }

                    // 聚合逻辑
                    @Override
                    public Tuple2<String, Integer> add(WaterSensor value, Tuple2<String, Integer> accumulator) {
//                        System.out.println("调用add方法,value=" + value);
                        return new Tuple2<>(value.getId(), accumulator.f1 + value.getVc());
                    }

                    // 获取最终结果，窗口触发时输出
                    @Override
                    public String getResult(Tuple2<String, Integer> accumulator) {
//                        System.out.println("调用getResult方法");
                        return "key=" + accumulator.f0 + " sum=" + accumulator.f1;
                    }

                    // 只有会话窗口才会用到
                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                }
        );
        aggregate.print();
        env.execute();
    }
}
