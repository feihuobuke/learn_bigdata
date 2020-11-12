//package sparkstreaming.ha.kafka._08;
//
//
//import kafka.common.TopicAndPartition;
//import org.apache.spark.streaming.kafka.KafkaCluster;
//import org.apache.spark.streaming.kafka.OffsetRange;
//import org.apache.spark.streaming.scheduler.*;
//import scala.Option;
//import scala.collection.JavaConversions;
//import scala.collection.immutable.List;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class NxListener implements StreamingListener {
//    private KafkaCluster  kc;
//    public scala.collection.immutable.Map<String, String>  kafkaParams;
//
//    public NxListener(scala.collection.immutable.Map<String, String> kafkaParams){
//        this.kafkaParams=kafkaParams;
//         kc = new KafkaCluster(kafkaParams);
//    }
//
//
//    @Override
//    public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
//
//    }
//
//    @Override
//    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
//
//    }
//
//    @Override
//    public void onReceiverError(StreamingListenerReceiverError receiverError) {
//
//    }
//
//    @Override
//    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
//
//    }
//
//    @Override
//    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
//
//    }
//
//    @Override
//    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
//
//    }
//
//    /**
//     * 批次完成时调用的方法
//     * @param batchCompleted
//     *
//     * batchCompleted 对象里面带有了偏移量对信息，所以我提交偏移量对时候
//     * 就是从这个对象里面读取offset就可以了。
//     *
//     *
//     *
//     */
//    @Override
//    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {//这个对象里面是有任务的offset信息
//        /**
//         * Batch： 小概率 -> 一年了
//         *  task1 offset  成功 100
//         *  task2 offset  成功 77
//         *  task3 offset  失败  66
//         *
//         *
//         */
//        //任务运行完了以后会（任务成功，）提交偏移量
//        //
//        //如果本批次里面有任务失败了，那么就终止偏移量提交
//        scala.collection.immutable.Map<Object, OutputOperationInfo> opsMap = batchCompleted.batchInfo().outputOperationInfos();
//
//        Map<Object, OutputOperationInfo> javaOpsMap = JavaConversions.mapAsJavaMap(opsMap);
//        for (Map.Entry<Object, OutputOperationInfo> entry : javaOpsMap.entrySet()) {
//            //failureReason不等于None(是scala中的None),说明有异常，不保存offset
//            //OutputOperationInfo task
//            //task -> failureReason
//            if (!"None".equalsIgnoreCase(entry.getValue().failureReason().toString())) {
//                return;
//            }
//        }
//
//
//      //  long batchTime = batchCompleted.batchInfo().batchTime().milliseconds();
//        /**
//         * topic，分区，偏移量
//         */
//        Map<String, Map<Integer, Long>> offset = getOffset(batchCompleted);
//
//        for (Map.Entry<String, Map<Integer, Long>> entry : offset.entrySet()) {
//            String topic = entry.getKey();
//            Map<Integer, Long> paritionToOffset = entry.getValue();
//            //我只需要这儿把偏移信息放入到zookeeper就可以了。
//            for(Map.Entry<Integer,Long> p2o : paritionToOffset.entrySet()){
//                Map<TopicAndPartition, Object> map = new HashMap<TopicAndPartition, Object>();
//
//                TopicAndPartition topicAndPartition =
//                        new TopicAndPartition(topic,p2o.getKey());
//
//                map.put(topicAndPartition,p2o.getValue());
//                scala.collection.immutable.Map<TopicAndPartition, Object>
//                        topicAndPartitionObjectMap = TypeHelper.toScalaImmutableMap(map);
//                //提交偏移量
//
//                //这个方法是 0.8的依赖提供的API
//                //这样话，就自动帮我把offset存到了ZK
//                kc.setConsumerOffsets(kafkaParams.get("group.id").get(), topicAndPartitionObjectMap);
//            }
//
//        }
//    }
//
//    @Override
//    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
//
//    }
//
//    @Override
//    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
//
//    }
//
//    private Map<String, Map<Integer, Long>> getOffset(StreamingListenerBatchCompleted batchCompleted) {
//        Map<String, Map<Integer, Long>> map = new HashMap<>();
//
//        scala.collection.immutable.Map<Object, StreamInputInfo> inputInfoMap =
//                batchCompleted.batchInfo().streamIdToInputInfo();
//        Map<Object, StreamInputInfo> infos = JavaConversions.mapAsJavaMap(inputInfoMap);
//
//        infos.forEach((k, v) -> {
//            Option<Object> optOffsets = v.metadata().get("offsets");
//            if (!optOffsets.isEmpty()) {
//                Object objOffsets = optOffsets.get();
//                if (List.class.isAssignableFrom(objOffsets.getClass())) {
//                    List<OffsetRange> scalaRanges = (List<OffsetRange>) objOffsets;
//
//                    Iterable<OffsetRange> ranges = JavaConversions.asJavaIterable(scalaRanges);
//                    for (OffsetRange range : ranges) {
//                        if (!map.containsKey(range.topic())) {
//                            map.put(range.topic(), new HashMap<>());
//                        }
//                        map.get(range.topic()).put(range.partition(), range.untilOffset());
//                    }
//                }
//            }
//        });
//
//        return map;
//    }
//
//
//}
