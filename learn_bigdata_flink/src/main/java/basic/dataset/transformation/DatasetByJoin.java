package basic.dataset.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des: 内连接
 */
public class DatasetByJoin {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "Ada"));
        data1.add(new Tuple2<>(2, "Reiser"));
        data1.add(new Tuple2<>(3, "Jedi"));

        List<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "Beijing"));
        data2.add(new Tuple2<>(2, "Shanghai"));
        data2.add(new Tuple2<>(3, "Shenzhen"));


        DataSource<Tuple2<Integer, String>> dataSource1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSource2 = env.fromCollection(data2);

        JoinOperator.EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> joinData
                = dataSource1.join(dataSource2)
                // 指定第一个数据集中需要进行比较的元素下标
                .where(0)
                // 指定第二个数据集中需要进行比较的元素下标
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

        joinData.print();

    }
}
