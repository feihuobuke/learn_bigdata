package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/31
 * Des: 5秒没有单词输出，则输出该单词的单词次数
 * <p>
 * 思路
 * <p>
 * 1. 利⽤state存储key，count和key到达的时间
 * 2. 没接收到⼀个单词，更新状态中的数据
 * 3. 对于每个key都注册⼀个定时器，如果过了5秒没接收到这个key到话，那么就触发这个定时器，这个定 时器就判断当前的event time是否等于这个key的最后修改时间+5s，如果等于则输出key以及对应的 count
 */
public class SesseionWindowWithCountByTimer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);

        // 3. Data Process
        // non keyed stream
        DataStream<Tuple2<String, Integer>> wordOnes = dataStreamSource.flatMap(new WordOneFlatMapFunction());

        // 3.2 按照单词进⾏分组, 聚合计算每个单词出现的次数
        // keyed stream
        KeyedStream<Tuple2<String, Integer>, Tuple> wordGroup = wordOnes.keyBy(0);

        wordGroup.process(new CountWithTimeoutFunction()).print();
        // 5. 启动并执⾏流程序
        env.execute("Streaming WordCount");
    }

    private static class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState", CountWithTimestamp.class));
        }

        /**
         * 处理每⼀个接收到的单词(元素)
         *
         * @param element 输⼊元素
         * @param ctx     上下⽂
         * @param out     ⽤于输出
         * @throws Exception
         */
        @Override
        public void processElement(Tuple2<String, Integer> element, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 拿到当前 key 的对应的状态
            CountWithTimestamp currentState = state.value();
            if (currentState == null) {
                currentState = new CountWithTimestamp();
                currentState.key = element.f0;
            }
            // 更新这个 key 出现的次数
            currentState.count++;

            // 更新这个 key 到达的时间，最后修改这个状态时间为当前的 Processing Time
            currentState.lastModified = ctx.timerService().currentProcessingTime();

            // 更新状态
            state.update(currentState);

            // 注册⼀个定时器
            // 注册⼀个以 Processing Time 为准的定时器
            // 定时器触发的时间是当前 key 的最后修改时间加上 5 秒
            ctx.timerService().registerProcessingTimeTimer(currentState.lastModified + 5000);

        }

        /**
         * 定时器需要运⾏的逻辑
         *
         * @param timestamp 定时器触发的时间戳
         * @param ctx       上下⽂ * @param out ⽤于输出
         * @throws Exception
         */

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 先拿到当前 key 的状态
            CountWithTimestamp curr = state.value();
            // 检查这个 key 是不是 5 秒钟没有接收到数据
            if (timestamp == curr.lastModified + 5000) {
                out.collect(Tuple2.of(curr.key, curr.count));
                state.clear();
            }

        }
    }

    private static class CountWithTimestamp {
        public String key;
        public int count;
        public long lastModified;
    }

    private static class WordOneFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.toLowerCase().split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }

        }
    }
}
