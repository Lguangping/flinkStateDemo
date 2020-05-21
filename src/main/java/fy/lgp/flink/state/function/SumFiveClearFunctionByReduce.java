package fy.lgp.flink.state.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * author : li guang ping
 * description : 实现统计函数, 每5次清零
 * date : 20-5-13 下午3:39
 **/
public class SumFiveClearFunctionByReduce extends RichMapFunction<String, Tuple2<String, Long>> {
    private static final Long ONE = 1L;
    private ReducingState<Long> sumState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ReducingStateDescriptor<Long> descriptor =
            new ReducingStateDescriptor<Long>(
                "SumFiveClearFunctionByReduce",  // 状态的名字
                new ReduceFunction<Long>() { // 聚合函数
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                }, Long.class); // 状态存储的数据类型
        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
        // 将数据放到状态中
        sumState.add(ONE);
        Long aLong = sumState.get();
        if (aLong == 5){
            sumState.clear();
        }
        return Tuple2.of(s, aLong);
    }
}
