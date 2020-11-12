package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 16:57
 * @Description: Similar to collect, but works on key-value RDDs and converts them into Scala
 *              maps to preserve their key-value structure.
 **/
object RDD_03_collectAsMap {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = Array[String]("huangbo", "xuzheng", "wangbaoqiang")
        val data2 = Array[Int](18, 19, 20)
        
        val rdd1: RDD[String] = sc.parallelize(data1)
        val rdd2: RDD[Int] = sc.parallelize(data2)
        
        val resultRDD: RDD[(String, Int)] = rdd1.zip(rdd2)
        
        val mapResult: collection.Map[String, Int] = resultRDD.collectAsMap()
        
        for (x <- mapResult) {
            println(x._1, x._2)
        }
    }
}
