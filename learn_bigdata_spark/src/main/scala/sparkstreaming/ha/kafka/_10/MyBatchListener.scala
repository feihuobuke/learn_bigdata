package sparkstreaming.ha.kafka._10

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, OffsetRange}
import org.apache.spark.streaming.scheduler._

class MyBatchListener(var stream: InputDStream[ConsumerRecord[String, String]]) extends StreamingListener {

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {

    val oInfo: OutputOperationInfo = batchCompleted.batchInfo.outputOperationInfos(2)

    // 成功的 task 的才提交
    if ("None".equalsIgnoreCase(oInfo.failureReason.toString())) {
      val info: Map[Int, StreamInputInfo] = batchCompleted.batchInfo.streamIdToInputInfo

      var offsetRangesTmp: List[OffsetRange] = null;
      var offsetRanges: Array[OffsetRange] = null;

      for (k <- info) {
        val offset: Option[Any] = k._2.metadata.get("offsets")

        if (!offset.isEmpty) {
          try {
            val offsetValue = offset.get
            offsetRangesTmp = offsetValue.asInstanceOf[List[OffsetRange]]
            offsetRanges = offsetRangesTmp.toSet.toArray;
          } catch {
            case e: Exception => println(e)
          }
        }
      }

      if (offsetRanges != null) {
        //提交偏移量（Kafka）
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges);
      }
    }
  }
}
