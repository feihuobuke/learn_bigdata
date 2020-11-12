package basic.dataset.transformation;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des: 类似map，一次处理一个分区的数据【如果在进行map处理的时候需要获取第三方资源 链接，建议使用MapPartition】
 */
public class DatasetByMapPartition {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> sourceData = env.fromCollection(new ArrayList<String>() {
            {
                add("hello Ada");
                add("hello Reiser");
            }
        });

        MapPartitionOperator<String, String> mapPartitionData = sourceData.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> output) throws Exception {
                //获取数据库连接--注意，此时是一个分区的数据获取一次连接【优点，每个分区获取 一次链接】
                //values中保存了一个分区的数据
                for (String next : values) {
                    String[] split = next.split("\\W+");

                    for (String word : split) {
                        output.collect(word);
                    }
                }
            }
        });

        mapPartitionData.print();

    }
}
