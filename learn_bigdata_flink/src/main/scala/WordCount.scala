import org.apache.flink.streaming.api.scala._

/**
 * scala 版本的 word count
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    data.flatMap(line => line.split(",")).map((_, 1)).keyBy(0).sum(1).print()

    env.execute("wordcount")
  }
}