package fy.lgp.flink.state.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * author : li guang ping
 * description : 每隔一秒发送一个数组内的字符串
 * date : 20-5-13 下午3:36
 **/
public class CustomSource implements SourceFunction<String> {
    private final static String[] array = new String[]{"hadoop", "flink", "spark", "hbase", "hive"};
    private final static Random random = new Random();
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true){
            TimeUnit.SECONDS.sleep(1);
            sourceContext.collect(
                array[random.nextInt(5)]
            );
        }
    }

    @Override
    public void cancel() {

    }
}
