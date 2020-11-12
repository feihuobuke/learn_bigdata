package state.samples;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import static state.samples.OrderInfo1.string2OrderInfo1;
import static state.samples.OrderInfo2.string2OrderInfo2;

/**
 * @author: reiserx
 * Date:2020/10/25
 * Des:
 */
public class OrderStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> info1 = env.addSource(new FileSource(Constants.ORDER_INFO1_PATH));

        DataStreamSource<String> info2 = env.addSource(new FileSource(Constants.ORDER_INFO2_PATH));

        KeyedStream<OrderInfo1, Long> orderInfo1Stream = info1.map(line -> string2OrderInfo1(line)).keyBy(orderInfo1 -> orderInfo1.getOrderId());

        KeyedStream<OrderInfo2, Long> orderInfo2Stream = info2.map(line -> string2OrderInfo2(line)).keyBy(orderInfo2 -> orderInfo2.getOrderId());

        orderInfo1Stream.connect(orderInfo2Stream).flatMap(new EnrichmentFunction()).print();

        env.execute("OrderStream");

    }

    /**
     * IN1, 第一个流的输入的数据类型 IN2, 第二个流的输入的数据类型 OUT，输出的数据类型
     */
    public static class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {
        //定义第一个流 key对应的 state
        private ValueState<OrderInfo1> orderInfo1State;
        //定义第二个流 key对应的 state
        private ValueState<OrderInfo2> orderInfo2State;

        @Override
        public void open(Configuration parameters) {
            orderInfo1State = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo1>("info1", OrderInfo1.class));
            orderInfo2State = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo2>("info2", OrderInfo2.class));

        }

        //不确定 orderInfo1 orderInfo2 谁先到
        @Override
        public void flatMap1(OrderInfo1 orderInfo1, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
            OrderInfo2 value2 = orderInfo2State.value();
            if (value2 != null) {
                orderInfo2State.clear();
                out.collect(Tuple2.of(orderInfo1, value2));
            } else {
                orderInfo1State.update(orderInfo1);
            }

        }

        @Override
        public void flatMap2(OrderInfo2 orderInfo2, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
            OrderInfo1 value1 = orderInfo1State.value();
            if (value1 != null) {
                orderInfo1State.clear();
                out.collect(Tuple2.of(value1, orderInfo2));
            } else {
                orderInfo2State.update(orderInfo2);
            }

        }

    }
}
