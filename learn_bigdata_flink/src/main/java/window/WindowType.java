package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/31
 * Des: Window 的类型
 * <p>
 */
public class WindowType {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }

        });

        //Non keyed Stream
        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> nonkeyedStream = stream.timeWindowAll(Time.seconds(3));
        nonkeyedStream.sum(1).print();

        //Keyed Stream
        stream.keyBy(0).timeWindow(Time.seconds(3)).sum(1).print();


        //滑动窗口（可重复）- time window
        stream.keyBy(0)
                .timeWindow(Time.seconds(3))
                .sum(1).print();

        //滚动窗口- time window
        stream.keyBy(0)
                /*
                Time.seconds(1) 窗口大小 start 和 end 的距离
                Time.seconds(30) 滑动距离 上一次 start 和这一次 start 的距离
                 */
                .timeWindow(Time.seconds(1), Time.seconds(30))
                .sum(1).print();


        //滑动窗口（可重复）- count window
        stream.keyBy(0)
                .countWindow(10)
                .sum(1).print();

        //滚动窗口- count window
        stream.keyBy(0)
                .countWindow(100, 10)
                .sum(1).print();


        //滑动窗口和滚动窗口的另外一种写法
        stream.keyBy(0)
                // 等于.timeWindow(Time.seconds(3))
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum(1).print();

        stream.keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(30)))
                .sum(1).print();

        env.execute("word count");

    }
}
