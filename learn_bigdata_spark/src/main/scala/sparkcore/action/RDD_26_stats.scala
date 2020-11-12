package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 14:48
 * @Description: Simultaneously computes the mean, variance and the standard deviation of all values in the RDD.
 **/
object RDD_26_stats {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        val data = List(1, 2, 3, 4, 5)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
    
        /*
         * TODO_MA 获取 rdd 的一些统计信息
         */
        val resultCounter: StatCounter = dataRDD.stats()
        
        println(resultCounter.max)
        println(resultCounter.min)
        println(resultCounter.mean)
        println(resultCounter.stdev)
        println(resultCounter.count)
        println(resultCounter.sum)
        println(resultCounter.variance)
        
    }
}
