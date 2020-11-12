package com.reiser.yeye.adclick

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
// 按照省份统计的输出结果样例类
case class CountByProvince( windowEnd: String, province: String, count: Long )
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )


/**
 * 广告点击统计
 */
object AdClickCount {

  private val outputBlackList = new OutputTag[BlackListWarning]("blackwaring")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1. 获取数据
    val adEventStream = env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data3.csv").map(data => {
      val dataArray = data.split(",")
      AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      .assignTimestampsAndWatermarks(new AdClickEventTimeExtractor())

    //2. 计算黑名单
    val filterBlackListStream = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))

    //3. 打印黑名单
    filterBlackListStream.getSideOutput(outputBlackList)
      .print()

    //4.计算各省份的广告点击量

    val adCountStream = filterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdClickCount, new AdClickWindow)

    adCountStream.print()


    env.execute("AdClickCount")

  }

  /**
   * 自定义窗口处理
   */
  class AdClickWindow extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
    override def apply(key: String, window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[CountByProvince]): Unit = {
      out.collect(CountByProvince(new Timestamp(window.getEnd).toString,
        key,input.iterator.next()))
    }
  }

  class AdClickCount extends AggregateFunction[AdClickEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: AdClickEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  /**
   * 过滤黑名单数据
   * @param maxCount
   */
  class FilterBlackListUser(maxCount:Int)
    extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    //保存当前用户对当前广告的点击量
    lazy val countState:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count-state",classOf[Long]))


    //保存是否发送黑名单的状态
    lazy val isSetBlackList:ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]))

    //保存定时器触发的时间戳
    lazy val resetTimeer:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("resettime-state",classOf[Long])
    )

    override def processElement(value: AdClickEvent,
                                ctx: KeyedProcessFunction[(Long, Long),
                                  AdClickEvent, AdClickEvent]#Context,
                                out: Collector[AdClickEvent]): Unit = {
      val currentCount = countState.value()
      //如果是第一次处理，注册定时器，每天00:00触发
      if(currentCount == 0){
        val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) +1) * (1000 * 60 * 60 * 24)
        resetTimeer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }
      //判断计数是否达到上线，如果达到加入黑名单
      if(currentCount >= maxCount){
        if(!isSetBlackList.value()){
          isSetBlackList.update(true)
          //输入到侧输出流
          ctx.output(outputBlackList,
            BlackListWarning(value.userId,value.adId,"Click over" + maxCount +" times"))
        }
        return
      }

      countState.update(currentCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[(Long, Long),
                           AdClickEvent, AdClickEvent]#OnTimerContext,
                         out: Collector[AdClickEvent]): Unit = {
      if(timestamp == resetTimeer.value()){
        isSetBlackList.clear()
        countState.clear()
        resetTimeer.clear()
      }

    }

  }

}

class AdClickEventTimeExtractor extends AssignerWithPeriodicWatermarks[AdClickEvent]{

  var currentMaxEventTime = 0L
  val maxOufOfOrderness = 10000 //最大乱序时间

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxEventTime - maxOufOfOrderness)
  }

  override def extractTimestamp(element: AdClickEvent, previousElementTimestamp: Long): Long = {
    //时间字段
    val timestamp = element.timestamp * 1000

    currentMaxEventTime = Math.max(element.timestamp, currentMaxEventTime)
    timestamp;
  }
}
