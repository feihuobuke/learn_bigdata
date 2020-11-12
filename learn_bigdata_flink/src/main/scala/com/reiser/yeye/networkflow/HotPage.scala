package com.reiser.yeye.networkflow


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object HotPage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data2.log")
      .map(line => {
        val fields = line.split(" ")
        val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timeStamp = dateFormat.parse(fields(3).trim).getTime
        ApacheLogEvent(fields(0).trim, fields(1).trim, timeStamp,
          fields(5).trim, fields(6).trim)
      })
      .assignTimestampsAndWatermarks(new HotPageEventTimeExtractor)
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageWindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNHotPage(5))
      .print()

    env.execute("hot page count")
  }

}

class TopNHotPage(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlState: MapState[String, Long] =
    getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-state", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    val iter = urlState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allUrlViews += ((entry.getKey, entry.getValue))
    }

    urlState.clear()

    val sortedUrlView = allUrlViews.sortWith(_._2 > _._2).take(topSize)

    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    sortedUrlView.foreach(view => {
      result.append("URL:").append(view._1)
        .append(" 访问量：").append(view._2).append("\n")
    })
    result.append("===================")

    out.collect(result.toString())
  }
}

class PageWindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

case class ApacheLogEvent(ip: String, userId: String,
                          eventTime: Long, method: String,
                          url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)


class HotPageEventTimeExtractor extends AssignerWithPeriodicWatermarks[ApacheLogEvent] {

  var currentMaxEventTime = 0L
  val maxOufOfOrderness = 10000 //最大乱序时间

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxEventTime - maxOufOfOrderness)
  }

  override def extractTimestamp(element: ApacheLogEvent, previousElementTimestamp: Long): Long = {
    //时间字段
    val timestamp = element.eventTime

    currentMaxEventTime = Math.max(element.eventTime, currentMaxEventTime)
    timestamp;
  }
}

