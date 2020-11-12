package basic.streaming.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author: reiserx
 * Date:2020/10/24
 * Des:
 */
public class StreamingBySourceFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hadoop");
        data.add("spark");
        data.add("flink");

        DataStreamSource<String> dataStream = env.fromCollection(data);

        SingleOutputStreamOperator<String> addPreStream = dataStream.map(s -> "collection_" + s);

        addPreStream.print().setParallelism(1);

        env.execute("StreamingSourceFromCollection");


    }
}
