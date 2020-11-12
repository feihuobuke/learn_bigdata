package basic.streaming.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: reiserx
 * Date:2020/10/24
 * Des:
 */
public class StreamingBySourceFromMyNoParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //会报错，因为 MyNoParalleSource 是单数据源
        //DataStreamSource<Long> dataStream = env.addSource(new MyNoParalleSource()).setParallelism(2);
        DataStreamSource<Long> dataStream = env.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<Long> addPreStream = dataStream.map(s -> {
            System.out.println("接收都数据" + s);
            return s;
        });

        SingleOutputStreamOperator<Long> filterStream = addPreStream.filter(aLong -> aLong % 2 == 0);
        filterStream.print().setParallelism(1);

        env.execute("StreamingSourceFromCollection");
    }
}
