package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import com.atguigu.functions.WaterSensorMapFunction;
import com.atguigu.utils.DataSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
 *
 * @author cjp
 * @version 1.0
 */
public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<WaterSensor> sensorDS = DataSourceUtil.getWaterSensorDataStreamSource(env);

        sensorDS.keyBy(WaterSensor::getId).process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 1.定义状态
                    ValueState<Integer> lastVcState;

                    // 初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 状态描述器两个参数：第一个参数，名字，不重复；第二个参数，存储的类型
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //lastVcState.value();  // 取出 本组 值状态 的数据
                        //lastVcState.update(); // 更新 本组 值状态 的数据
                        //lastVcState.clear();  // 清除 本组 值状态 的数据

                        // 1. 取出上一条数据的水位值(Integer默认值是null，判断)
                        int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        // 2. 求差值的绝对值，判断是否超过10
                        Integer vc = value.getVc();
                        if (Math.abs(vc - lastVc) > 10) {
                            out.collect("传感器=" + value.getId() + "==>当前水位值=" + vc + ",与上一条水位值=" + lastVc + ",相差超过10！！！！");
                        }
                        // 3. 更新状态里的水位值
                        lastVcState.update(vc);
                    }
                }
        ).print();
        env.execute();
    }
}
