package com.reiser.stream.mock;

import com.reiser.stream.entity.AdClientLog;
import com.reiser.stream.entity.AdLog;
import com.reiser.stream.entity.AdServerLog;
import com.reiser.stream.entity.schema.AdClientLogSchema;
import com.reiser.stream.untils.Constants;
import com.reiser.stream.untils.FlinkKafkaConsumerUtils;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author: reiserx
 * Date:2020/11/20
 * Des:
 */
public class Test {
    private static String KAFKA_SERVER_LOG = Constants.SERVER_LOG;
    private static String KAFKA_CLIENT_LOG = Constants.CLIENT_LOG;
    private static String KAFKA_AD_LOG = Constants.AD_LOG;
    private static String KAFKA_AD_LOG_REPORT = Constants.AD_LOG_REPORT;
    private static String BROKERS = Constants.BROKERS;

    public static void main(String[] args) throws Exception {
        String groupId = "flink-join-test";

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(60000);
        env.setParallelism(2);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // TODO 什么可以只设置使用 EventTime，而不需要使用 WaterMaker
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableForceAvro();

        env.getConfig().registerTypeWithKryoSerializer(AdClientLog.class, ProtobufSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AdServerLog.class, ProtobufSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AdLog.class, ProtobufSerializer.class);


        // 1.处理 serverLog 到 redis 和 hbase
//        Properties serverLogProperties = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_SERVER_LOG, groupId);
//        DataStreamSource<AdServerLog> adServerLogDataStreamSource = env.addSource(new FlinkKafkaConsumer010<>(KAFKA_SERVER_LOG, new AdServiceLogSchema(), serverLogProperties));
//        adServerLogDataStreamSource.print();


        Properties clientLogProperties = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_CLIENT_LOG, groupId);
        DataStreamSource<AdClientLog> adClientLogDataStreamSource = env.addSource(new FlinkKafkaConsumer010<>(KAFKA_CLIENT_LOG, new AdClientLogSchema(), clientLogProperties));
        adClientLogDataStreamSource.print();


        env.execute();

    }
}
