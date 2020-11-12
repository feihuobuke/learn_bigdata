package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:28
 * @Description: Orders the data items of the RDD using their inherent implicit ordering function
 *              and returns the first n items as an array.
 **/
object RDD_08_takeOrdered {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List(2, 8, 7, 5, 6, 9, 4, 2, 5, 4, 18, 3, 5, 9)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
        
        /*
         * TODO_MA 按照元素的默认顺序排序之后，取n个元素
         *      可以理解成取最小的n个元素
         */
        val resultValue: Array[Int] = dataRDD.takeOrdered(3)
        
        resultValue.foreach(println)
    }
}
