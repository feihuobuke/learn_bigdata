package state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:
 */
public class KeyedStateWithValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 2L), Tuple2.of(2L, 4L), Tuple2.of(2L, 6L));

        dataStreamSource
                .keyBy(0)
                .flatMap(new CountWindowAverageWithValueState())
                .print();

        env.execute("TestStatefulApi");
    }
}

class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 获取 ValueState 的描述
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<Tuple2<Long, Long>>("average", Types.TUPLE(Types.LONG, Types.LONG));
//        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>( "average", // 状态的名字 String.class, Long.class);
//        ListStateDescriptor<Tuple2<Long, Long>> descriptor = new ListStateDescriptor<Tuple2<Long, Long>>( "average", // 状态的名字 Types.TUPLE(Types.LONG, Types.LONG));

        //设置过期时间
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                //UpdateType 表明了过期时间什么时候更新
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //StateVisibility 对于那些过期的状态，是否还能被访问
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        countAndSum = getRuntimeContext().getState(descriptor);


    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {

        // 拿到当前的 key 的状态值
        Tuple2<Long, Long> currentState = countAndSum.value();

        if (currentState == null) {
            currentState = Tuple2.of(0L, 0L);
        }
        currentState.f0 += 1;
        currentState.f1 += element.f1;
        countAndSum.update(currentState);
        if (currentState.f0 >= 3) {
            double avg = (double) currentState.f1 / currentState.f0;
            out.collect(Tuple2.of(element.f0, avg));
            countAndSum.clear();
        }
    }
}
