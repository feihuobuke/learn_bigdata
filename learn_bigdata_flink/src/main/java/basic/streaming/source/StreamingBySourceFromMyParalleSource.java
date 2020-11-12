package basic.streaming.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: reiserx
 * Date:2020/10/24
 * Des:
 */
public class StreamingBySourceFromMyParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dataStream = env.addSource(new MyParalleSource()).setParallelism(4);

        SingleOutputStreamOperator<Long> addPreStream = dataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long s) throws Exception {
                System.out.println("接收都数据" + s);
                return s;
            }
        });

        SingleOutputStreamOperator<Long> filterStream = addPreStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 2 == 0;
            }
        });
        filterStream.print().setParallelism(1);

        env.execute("StreamingSourceFromCollection");
    }
}
