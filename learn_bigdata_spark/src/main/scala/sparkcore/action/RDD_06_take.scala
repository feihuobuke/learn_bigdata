package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:27
 * @Description: Extracts the first n items of the RDD and returns them as an array.
 *              (Note: This sounds very easy, but it is actually quite a tricky problem for the
 *              implementors of Spark because the items in question can be in many different partitions.)
 **/
object RDD_06_take {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List(2, 8, 7, 5, 6, 9, 4, 2, 5, 4, 18, 3, 5, 9)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
        
        /*
         * TODO_MA Extracts the first n items of the RDD and returns them as an array.
         */
        val resultValue: Array[Int] = dataRDD.take(3)
        
        resultValue.foreach(println)
    }
}
