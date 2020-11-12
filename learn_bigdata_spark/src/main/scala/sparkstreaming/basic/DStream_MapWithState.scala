package sparkstreaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
 * MapWithState算子
 * 效果和 UpdateStateByKey 一样
 * 比 UpdateStateByKey 性能好 5-10 倍
 * 应用：有状态的 DStream
 */
object DStream_MapWithState {

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

    val initialRDD = ssc.sparkContext.parallelize(List(("hadoop", 100), ("flink", 20)))
    /**
     * 定义一个函数，该函数有三个类型word: String, option: Option[Int], state: State[Int]
     * 其中word代表统计的单词，option代表的是历史数据（使用option是因为历史数据可能有，也可能没有，如第一次进来的数据就没有历史记录），state代表的是返回的状态
     */


    val stateSpec = StateSpec.function((word: String, option: Option[Int], state: State[Int]) => {
      if (state.isTimingOut()) {
        println(word + "is timeout")
      } else {
        // getOrElse(0)不存在赋初始值为零
        val sum = option.getOrElse(0) + state.getOption().getOrElse(0)
        // 单词和该单词出现的频率/ 获取历史数据，当前值加上上一个批次的该状态的值
        val wordFreq = (word, sum)
        // 更新状态
        state.update(sum)
        wordFreq
      }
    }).initialState(initialRDD)
      .timeout(Seconds(30))

    val wordCount = wordAndOneDStream.mapWithState(stateSpec)



    //4.输出数据
    wordCount.stateSnapshots().print()

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
