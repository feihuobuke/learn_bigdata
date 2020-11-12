package window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: reiserx
 * Date:2020/11/3
 * Des:window join function
 * 1,a
 */
public class WindowWithJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, String>> wordStream = sourceStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple2.of(words[0], words[1]);
            }
        });


        DataStreamSource<String> sourceStream2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> wordStream2 = sourceStream2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] words = value.split(",");
                return Tuple2.of(words[0], words[1]);
            }
        });

        wordStream.join(wordStream2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> t1,
                                       Tuple2<String, String> t2) throws Exception {
                        System.out.println("t1 " + t1.toString() + "t2 " + t2.toString());

                        return t1.toString() + t2.toString();
                    }
                }).print();


        env.execute(WindowWithIncrAgg.class.getSimpleName());
    }
}
