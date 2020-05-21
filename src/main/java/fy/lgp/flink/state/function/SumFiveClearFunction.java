package fy.lgp.flink.state.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.HashMap;
import java.util.Map;

/**
 * author : li guang ping
 * description : 实现统计函数, 每5次清零
 * date : 20-5-13 下午3:39
 **/
public class SumFiveClearFunction implements CheckpointedFunction, MapFunction<String, Tuple2<String, Long>> {

    // 用于缓存所有数据
    private Map<String, Long> bufferMap = new HashMap<>();
    // 用于保存内存中的状态信息
    private ListState<Map<String, Long>> checkpointState;

    private static final Long ONE = 1L;


    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
        Long aLong = bufferMap.get(s);
        if (aLong == null) {
            aLong = ONE;
        } else {
            aLong += ONE;
        }
        if (aLong == 5) {
            bufferMap.remove(s);
        } else {
            bufferMap.put(s, aLong);
        }
        return Tuple2.of(s, aLong);
    }

    // 用于将内存中数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        checkpointState.add(bufferMap);
    }

    // 用于在程序恢复的时候从状态中恢复数据到内存
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Map<String, Long>> descriptor =
            new ListStateDescriptor<Map<String, Long>>(
                "SumFiveClearFunction",
                TypeInformation.of(new TypeHint<Map<String, Long>>() {
                }));
        checkpointState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            Iterable<Map<String, Long>> maps = checkpointState.get();
            for (Map<String, Long> ele : maps) {
                bufferMap = ele;
            }
        }
    }
}
