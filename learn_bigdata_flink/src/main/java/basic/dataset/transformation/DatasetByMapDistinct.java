package basic.dataset.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des: 去重
 */
public class DatasetByMapDistinct {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> sourceData = env.fromCollection(new ArrayList<String>() {
            {
                add("hello Ada");
                add("hello Reiser");
            }
        });

        FlatMapOperator<String, String> wordsData = sourceData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String values, Collector<String> output) throws Exception {
                for (String word : values.toLowerCase().split("\\W+")) {
                    System.out.println("单词：" + word);
                    output.collect(word);
                }
            }
        });
        
        wordsData.distinct().print();

    }
}
