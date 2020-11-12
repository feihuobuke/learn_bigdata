package basic.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author: reiserx
 * Date:2020/10/19
 * Des:数据源修改为 kafka
 */
public class WordCount4WithKafka {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置 kafka 的数据源
        String topic = "test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.91.101:9092");
        properties.setProperty("group.id", "testConsumer");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        //步骤二：获取数据源
        DataStreamSource<String> dataStream = env.addSource(myConsumer).setParallelism(2);
        //步骤三：数据处理
        SingleOutputStreamOperator<WordAndCount> wordCount = dataStream.flatMap(new SplitLine()).keyBy("word").sum("count");
        wordCount.print();
        //步骤四：数据输出 wordCount.print(); //步骤五：启动任务
        env.execute("word count ...");

    }

    public static class SplitLine implements FlatMapFunction<String, WordAndCount> {

        @Override
        public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
            String[] fields = s.split(",");
            for (String field : fields) {
                collector.collect(new WordAndCount(field, 1));
            }
        }
    }


    public static class WordAndCount {
        private String word;
        private int count;

        public WordAndCount() {

        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public WordAndCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
