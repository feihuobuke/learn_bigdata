package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 14:21
 * @Description:
 **/
object RDD_29_zipWithUniqueId {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        /*
         * TODO_MA This is different from zipWithIndex since just gives a unique id to each data element
         *  but the ids may not match the index number of the data element.
         *  This operation does not start a spark job even if the RDD is spread across multiple partitions.
         *  Compare the results of the example below with that of the 2nd example of zipWithIndex.
         *  You should be able to see the difference.
         */
        val rdd = sc.parallelize(100 to 120, 5)
        val resultRDD: RDD[(Int, Long)] = rdd.zipWithUniqueId
        
        resultRDD.foreach(println)
    }
}
