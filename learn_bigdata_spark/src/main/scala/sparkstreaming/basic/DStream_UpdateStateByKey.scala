package sparkstreaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * UpdateStateByKey算子
 *
 */
object DStream_UpdateStateByKey {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //1.获取环境变量
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCountStreaming")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_flink/src/main/resources")
    //2.获取数据
    val lines = ssc.socketTextStream("localhost", 8888)

    //3.处理数据
    val words = lines.flatMap(_.split(","))

    val wordAndOneDStream = words.map(t => (t, 1))

    def updateFunction(values: Seq[Int], state: Option[Int]): Option[Int] = {
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount + lastCount)
    }
    // 使用 updateStateByKey 读取历史记录
    val result = wordAndOneDStream.updateStateByKey(updateFunction)
    //4.输出数据
    result.print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
