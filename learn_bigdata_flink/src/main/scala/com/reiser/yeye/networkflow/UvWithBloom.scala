package com.reiser.yeye.networkflow

import java.sql.Timestamp

import com.reiser.yeye.product.com.zeye.product.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 *   redis-server.exe redis.windows.conf
 *
 *   redis-cli.exe -h 127.0.0.1 -p 6379
 *
 *   flushall
 *
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据
    val dataStream = env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data1.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new PageViewEventTimeExtractor())
      .filter(_.behavior == "pv") // 只统计pv操作
      .map(data => ("key", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    dataStream.print()

    env.execute("uv with bloom job")
  }
}

// 自定义窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

// 定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  // 位图的总大小，默认16M
  //16 * 1024 * 1024 * 8
  //4     10     10    3

  //1后面27个0
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash函数的结果，当做位图的offset
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for( i <- 0 until value.length ){
      result += result * seed + value.charAt(i)
    }
    //他们之间进行&运算结果一定在位图之间
    result  & ( cap - 1 ) //0后面27个1
  }
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvInfo, String, TimeWindow]{
  // 定义redis连接
  lazy val jedis = new Jedis("localhost", 6379)

  lazy val bloom = new Bloom(1<<27)

  override def process(key: String, context: Context,
                       elements: Iterable[(String, Long)], out: Collector[UvInfo]): Unit = {
    // 位图的存储方式，key是windowEnd，value是bitmap
    //val storeKey = context.window.getEnd.toString
    val storeKey =new Timestamp(context.window.getEnd).toString

    var count = 0L
    // 把每个窗口的uv count值也存入名为count的redis表，
    // 存放内容为（windowEnd -> uvCount），所以要先从redis中读取
    if( jedis.hget("count", storeKey) != null ){
      count = jedis.hget("count", storeKey).toLong
    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    // 定义一个标识位，判断reids位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect( UvInfo(new Timestamp(context.window.getEnd).toString, count + 1) )
    } else {
      out.collect( UvInfo(new Timestamp(context.window.getEnd).toString, count) )
    }
  }
}

case class UvInfo(windowEnd:String,uvCount:Long)

/**
 * 定义waterMark
 */
class PageViewEventTimeExtractor extends  AssignerWithPeriodicWatermarks[UserBehavior] {


  var currentMaxEventTime = 0L
  val maxOufOfOrderness = 10000 //最大乱序时间

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxEventTime - maxOufOfOrderness)
  }

  override def extractTimestamp(element: UserBehavior, previousElementTimestamp: Long): Long = {
    //时间字段
    val timestamp = element.timestamp * 1000

    println(element.toString)
    currentMaxEventTime = Math.max(element.timestamp, currentMaxEventTime)
    timestamp;
  }
}


