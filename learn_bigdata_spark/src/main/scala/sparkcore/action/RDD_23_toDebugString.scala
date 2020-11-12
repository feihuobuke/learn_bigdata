package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 12:44
 * @Description: Returns a string that contains debug information about the RDD and its dependencies.
 **/
object RDD_23_toDebugString {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        val data = List(2, 8, 7, 5, 6, 9, 4, 2, 5, 4, 18, 3, 5, 9)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
    
        println(dataRDD.toDebugString)
    }
}
