package com.reiser.stream.ad;

import com.reiser.stream.entity.AdLog;
import com.reiser.stream.untils.Constants;
import com.twitter.chill.protobuf.ProtobufSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des: Service Log 和 Client Log join
 */
public class StreamJoinRetryJob {

    private static String KAFKA_CLIENT_LOG_RETRY = Constants.CLIENT_LOG;
    private static String KAFKA_AD_LOG = Constants.AD_LOG;
    private static String KAFKA_AD_LOG_REPORT = Constants.AD_LOG_REPORT;
    private static String BROKERS = Constants.BROKERS;

    public static void main(String[] args) throws Exception {
        String groupId = "flink-join-retry-test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(60000);
        env.setParallelism(2);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableForceAvro();
        env.getConfig().registerTypeWithKryoSerializer(AdLog.class, ProtobufSerializer.class);


//        // 1.处理 client log join service log 生成 ad log
//        Properties properties2 = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, KAFKA_CLIENT_LOG_RETRY, groupId);
//        DataStreamSource<AdLog> adLogInputStream = env.addSource(new FlinkKafkaConsumer010<>(KAFKA_CLIENT_LOG_RETRY, new AdLogSchema(), properties2));
//
//        //重试逻辑
//        SingleOutputStreamOperator<AdLog> adLogStream = adLogInputStream.keyBy(AdLog::getRequestId)
//                .timeWindow(Time.seconds(2))
//                .trigger(PurgingTrigger.of(ProcessingTimeTrigger.create()))
//                .apply(new AdLogRetryRichFlatMap());
//
//        // 2.sink retry ad log
//        Properties producerProperties = FlinkKafkaProducerUtils.getProducerProperties(BROKERS);
//        FlinkKafkaProducer010<AdLog> adLogSink = new FlinkKafkaProducer010<>(KAFKA_AD_LOG, new AdLogSchema(), producerProperties);
//        adLogStream.addSink(adLogSink).name("AdLogProcesser");
//
//
//        env.execute("Stream join retry job");
    }
}
