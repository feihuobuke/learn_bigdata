package com.reiser.yeye.product

package com.zeye.product

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class UserBehavior(userId: Long, productId: Long, categoryId: Int,
                        behavior: String, timestamp: Long)

case class ProductViewCount(productId: Long, windowEnd: Long, count: Long)

object HotProduct {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.readTextFile("/Users/reiserx/code/bigdata/learn_bigdata/learn_bigdata_flink/src/main/resources/data1.csv")
      .map(line => {
        val fields = line.split(",")
        UserBehavior(fields(0).trim.toLong,
          fields(1).trim.toLong, fields(2).trim.toInt,
          fields(3).trim, fields(4).trim.toLong)
      }).assignTimestampsAndWatermarks(new ProductEventTimeExtractor)
      .filter(_.behavior == "pv")
      .keyBy(_.productId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new ProductCount, new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopHotProduct(5))
      .print()

    env.execute("ZzHotProduct")
  }


}


class TopHotProduct(topN: Int)
  extends KeyedProcessFunction[Long, ProductViewCount, String] {

  private var productState: ListState[ProductViewCount] = _

  override def open(parameters: Configuration): Unit = {
    productState = getRuntimeContext.getListState(
      new ListStateDescriptor[ProductViewCount]("product-state",
        classOf[ProductViewCount]) //Student.class == classOf[Student]
    )
  }


  override def processElement(value: ProductViewCount,
                              context: KeyedProcessFunction[Long, ProductViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    productState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ProductViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    var allProduct: ListBuffer[ProductViewCount] = new ListBuffer[ProductViewCount]

    val iterator = productState.get().iterator()

    while (iterator.hasNext) {
      allProduct += iterator.next()
    }

    val sortedProduct = allProduct.sortWith(_.count > _.count).take(5)

    allProduct.clear()
    //把结果拼接成为字符串，输出去

    val sb = new StringBuilder()
    sb.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    sortedProduct.foreach(product => {
      sb.append(" 商品ID：").append(product.productId)
        .append(" 商品浏览量=").append(product.count)
        .append("\n")
    })

    sb.append("============================")
    out.collect(sb.toString())
  }
}

/**
 * IN, OUT, KEY, W <: Window
 */
class WindowResult() extends WindowFunction[Long, ProductViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long],
                     out: Collector[ProductViewCount]): Unit = {
    out.collect(new ProductViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class ProductCount extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc
}


class ProductEventTimeExtractor
  extends AssignerWithPeriodicWatermarks[UserBehavior] {

  var currentMaxEventTime = 0L
  val maxOutputOrderness = 10000

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxEventTime * 1000 - maxOutputOrderness)
  }

  override def extractTimestamp(userBehavior: UserBehavior, l: Long): Long = {
    currentMaxEventTime = Math.max(userBehavior.timestamp, currentMaxEventTime)
    userBehavior.timestamp * 1000
  }
}

