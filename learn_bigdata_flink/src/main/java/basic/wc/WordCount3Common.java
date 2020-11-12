package basic.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/19
 * Des:flink 平时的编程习惯
 *
 */
public class WordCount3Common {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         * 设置的任务并行数 Parallelism 不能大于集群的 slot
         */
        env.setParallelism(2);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname", "localhost");
        int port = parameterTool.getInt("port", 9999);

        //步骤二：获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port);
        //步骤三：数据处理
        /*
         * 实际工作中习惯 将结果封装成对象 和 自定义匿名内部类
         */
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
