package sparkstreaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Driver 高可用
 * 1.设置重启 3-5
 * 2.checkpoint
 *
 * 不推荐使用，因为会导致丢数据，目前企业方案是 SparkStreaming + Kafka 实现高可用
 */
object DStream_DriverHA {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //1.获取环境变量
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCountStreaming")
    val ssc = new StreamingContext(conf, Seconds(2))
    //2.获取数据
    val lines = ssc.socketTextStream("localhost", 8888)

    //3.处理数据
    val words = lines.flatMap(_.split(","))

    val pairs = words.map(t => (t, 1))

    val workCount = pairs.reduceByKey(_ + _)
    //4.输出数据
    workCount.print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
