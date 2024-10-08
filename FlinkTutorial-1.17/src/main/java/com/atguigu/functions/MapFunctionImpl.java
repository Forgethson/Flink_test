package com.atguigu.functions;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 用户自定义函数（UDF）
 *
 * @author cjp
 * @version 1.0
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
