package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: reiserx
 * Date:2020/11/3
 * Des:增量聚合
 * 窗⼝中每进⼊⼀条数据，就进⾏⼀次计算，等时间到了展示最后的结果 常⽤的聚合算⼦
 *
 * 常用算子：reduce(reduceFunction) aggregate(aggregateFunction) sum(),min(),max()
 */
public class WindowWithIncrAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> reduceStream = sourceStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).timeWindowAll(Time.seconds(10)).reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer last, Integer current) throws Exception {
                return last + current;
            }
        });

        reduceStream.print();
        env.execute(WindowWithIncrAgg.class.getSimpleName());
    }
}
