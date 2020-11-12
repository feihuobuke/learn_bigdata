package basic.dataset.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des: 广播变量
 */
public class DatasetByBroadCast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs", 18));
        broadData.add(new Tuple2<>("ls", 20));
        broadData.add(new Tuple2<>("ww", 17));
        broadData.add(new Tuple2<>("mm", 24));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> res = new HashMap<>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        MapPartitionOperator<String, String> result = data.mapPartition(new RichMapPartitionFunction<String, String>() {
            Map<String, Integer> allMap = new HashMap<>();
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (Map map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                for (String value : values) {
                    out.collect(value + "," + allMap.get(value));
                }

            }
        }).withBroadcastSet(toBroadcast, "broadCastMapName");
        result.print();

    }

}
