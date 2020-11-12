package sparkstreaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * transform 算子
 * 把 Dstream 转换成 RDD，
 * 应用：过滤黑名单
 */
object DStream_Transform {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //1.获取环境变量
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCountStreaming")
    val ssc = new StreamingContext(conf, Seconds(2))
    //    ssc.checkpoint("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_flink/src/main/resources")
    //2.获取数据
    val lines = ssc.socketTextStream("localhost", 8888)

    //3.处理数据
    val words = lines.flatMap(_.split(","))

    val wordAndOneDStream = words.map(t => (t, 1))

    //  过滤 "?", "!", "&"
    val filterRDD = ssc.sparkContext.parallelize(List("?", "!", "&")).map((_, true))
    // 把给名单广播出去
    val filterBoradcast = ssc.sparkContext.broadcast(filterRDD.collect())

    val filterWordCount = wordAndOneDStream.transform(rdd => {
      val filter = ssc.sparkContext.parallelize(filterBoradcast.value)

      val result = rdd.leftOuterJoin(filter)
      // join
      val filterResult = result.filter(tuple => {
        val word = tuple._1
        val joinResult = tuple._2
        joinResult._2.isEmpty
      })
      filterResult.map(tuple => (tuple._1, 1))
    })

    //4.输出数据
    filterWordCount.print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
