package com.reiser.stream.sink;

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
 * Date:2020/11/24
 * Des:
 */
public class SinkToHiveJob {
    private static String KAFKA_SERVER_LOG = Constants.SERVER_LOG;
    private static String KAFKA_CLIENT_LOG = Constants.CLIENT_LOG;
    private static String KAFKA_AD_LOG = Constants.AD_LOG;
    private static String BROKERS = Constants.BROKERS;
    private static String partition = "'dt='yyyyMMdd/'hour'=HH";

    public static void main(String[] args) {

        String groupId = "flink-sink-task-test2";

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableForceAvro();
        env.getConfig().registerTypeWithKryoSerializer(AdServerLog.class, ProtobufSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AdClientLog.class, ProtobufSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(AdLog.class, ProtobufSerializer.class);

        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_SERVER_LOG, groupId);
        Properties properties2 = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_CLIENT_LOG, groupId);
        Properties properties3 = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_AD_LOG, groupId);

        DataStreamSource<AdServerLog> adServerInputStream = env.addSource(

                new FlinkKafkaConsumer010<>(KAFKA_SERVER_LOG, new AdServerLogSchema(), properties));

        DataStreamSource<AdClientLog> adClientInputStream = env.addSource(

                new FlinkKafkaConsumer010<>(KAFKA_CLIENT_LOG, new AdClientLogSchema(), properties2));
    }

}
