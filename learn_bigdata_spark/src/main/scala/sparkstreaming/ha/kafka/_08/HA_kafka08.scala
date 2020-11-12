//package sparkstreaming.ha.kafka._08
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import kafka.serializer.StringDecoder
//
///**
// * SparkStreaming 和 kafka 整合
// * 并使用 offset 进行现场恢复
// *
// */
//object DStream_HA_kafka08 {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    //步骤一：建立程序入口
//    val conf = new SparkConf().setMaster("local[3]").setAppName("wordCount")
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    val ssc = new StreamingContext(conf, Seconds(5))
//    //2.获取数据
//
//    //设置各种参数
//    val brokers = "192.168.1x7.254:9092"
//    val topics = "class3"
//    val groupId = "class3_consumer" //注意，这个也就是我们的消费者的名字
//
//    val topicsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokers,
//      "group.id" -> groupId,
//      "enable.auto.commit" -> "false"
//    )
//    /**
//     * 关键步骤一：设置监听器，帮我们完成偏移量的提交
//     * 监听器的作用就是，我们每次运行完一个批次，就帮我们提交一次偏移量。
//     *
//     */
//    ssc.addStreamingListener(new NxListener(kafkaParams));
//
//    //关键步骤二： 创建对象，然后通过这个对象获取到上次的偏移量，然后获取到数据流
//    val km = new KafkaManager(kafkaParams)
//
//    //步骤三：创建一个程序入口//从上一次 偏移量开始消费
//    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicsSet)
//    // messages 还带有偏移量信息
//
//    //3.处理数据
//    messages
//      .map(_._2) //
//      .flatMap(_.split(","))
//      .map((_, 1))
//      .foreachRDD(rdd => {
//        rdd.foreach(line => {
//          //             println(line)
//          //
//          //             println("-==============进行业务处理就可以了=====================batch=========")
//          //             //就把处理结果存储到Mysql，hbase,kafka
//        })
//      })
//
//    //4.启动任务
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
