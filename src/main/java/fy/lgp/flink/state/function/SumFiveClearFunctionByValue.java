package fy.lgp.flink.state.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * author : li guang ping
 * description : 实现统计函数, 每5次清零
 * date : 20-5-13 下午3:39
 **/
public class SumFiveClearFunctionByValue extends RichMapFunction<String, Tuple2<String, Long>> {
    private static final Long ONE = 1L;
    private ValueState<Long> countAndSum;


    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<Long>(
                "SumFiveClearFunctionByValue",  // 状态的名字
                Types.LONG); // 状态存储的数据类型
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
        Long aLong = countAndSum.value();
        if (aLong == null) {
            aLong = ONE;
        } else {
            aLong = aLong + ONE;
        }
        if (aLong == 5) {
            countAndSum.clear();
        }else {
            countAndSum.update(aLong);
        }
        return Tuple2.of(s, aLong);
    }
}
