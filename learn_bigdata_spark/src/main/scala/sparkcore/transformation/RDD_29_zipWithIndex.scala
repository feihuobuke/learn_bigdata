package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 12:56
 * @Description: Zips the elements of the RDD with its element indexes.
 *              The indexes start from 0. If the RDD is spread across multiple partitions
 *              then a spark Job is started to perform this operation.
 *
 * def zipWithIndex(): RDD[(T, Long)]
 **/
object RDD_29_zipWithIndex {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = Array[String]("huangbo", "xuzheng", "wangbaoqiang")
        val data2 = Array[Int](18, 19, 20)
        
        val rdd1: RDD[String] = sc.parallelize(data1)
        val rdd2: RDD[Int] = sc.parallelize(data2)
        
        /*
         * TODO_MA 给当前 RDD 的每个元素，生成一个索引变成 一个二元组
         */
        val resultRDD: RDD[(String, Long)] = rdd1.zipWithIndex()
        
        resultRDD.foreach(x => println(x))
    }
}
