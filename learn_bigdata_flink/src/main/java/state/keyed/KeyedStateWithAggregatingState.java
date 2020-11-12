package state.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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
public class KeyedStateWithAggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 2L), Tuple2.of(2L, 4L), Tuple2.of(2L, 6L));


        dataStreamSource
                .keyBy(0)
                .flatMap(new ContainValueFunction())
                .print();

        env.execute("TestStatefulApi");
    }
}

class ContainValueFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {
    // managed keyed state
    private AggregatingState<Long, String> totalStr;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AggregatingStateDescriptor<Long, String, String> descriptor = new AggregatingStateDescriptor<>("totalStr", new AggregateFunction<Long, String, String>() {
            @Override
            public String createAccumulator() {
                return "content: ";
            }

            @Override
            public String add(Long value, String accumulator) {
                if ("content: ".equals(accumulator)) {
                    return accumulator + value;
                }
                return accumulator + " and " + value;
            }

            @Override
            public String getResult(String accumulator) {
                return accumulator;
            }

            @Override
            public String merge(String a, String b) {
                return a + " and " + b;
            }
        }, String.class);

        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> out) throws Exception {
        totalStr.add(element.f1);
        out.collect(Tuple2.of(element.f0, totalStr.get()));

    }
}
