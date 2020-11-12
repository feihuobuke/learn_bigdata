package basic.dataset.transformation;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des: Counter
 */
public class DatasetByCounter {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            //1:创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(8);
        //如果要获取counter的值，只能是任务
        //result.print();
        result.writeAsText("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_flink/src/main/resources/mycounter");
        JobExecutionResult jobResult = env.execute("counter");
        //3：获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:" + num);

    }

}
