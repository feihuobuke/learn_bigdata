package window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author: reiserx
 * Date:2020/11/3
 * Des:增量聚合
 * 窗⼝中每进⼊⼀条数据，就进⾏⼀次计算，等时间到了展示最后的结果 常⽤的聚合算⼦
 * <p>
 * 常用算子：reduce(reduceFunction) aggregate(aggregateFunction) sum(),min(),max()
 */
public class WindowWithIncrAggWithMyAggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 8888);
        AllWindowedStream<Integer, TimeWindow> intStream = sourceStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).timeWindowAll(Time.seconds(10));

        intStream.aggregate(new MyAggregate()).print();

        env.execute(WindowWithIncrAggWithMyAggregate.class.getSimpleName());
    }

    private static class MyAggregate implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
            //个数+1
            //总的值累计
            return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return (double) (accumulator.f1 / accumulator.f0);
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
