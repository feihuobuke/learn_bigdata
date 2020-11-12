package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 17:00
 * @Description: Joins two RDDs by combining the i-th of either partition with each other.
 *              The resulting RDD will consist of two-component tuples
 *              which are interpreted as key-value pairs by the methods
 *              provided by the PairRDDFunctions extension.
 **/
object RDD_29_zip {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = Array[String]("huangbo", "xuzheng", "wangbaoqiang")
        val data2 = Array[Int](18, 19, 20)
        
        val rdd1: RDD[String] = sc.parallelize(data1)
        val rdd2: RDD[Int] = sc.parallelize(data2)
        
        /*
         * TODO_MA 两个RDD进行压缩，变成一个二元组RDD
         */
        val resultRDD: RDD[(String, Int)] = rdd1.zip(rdd2)
        
        resultRDD.foreach(x => println(x))
    }
}
