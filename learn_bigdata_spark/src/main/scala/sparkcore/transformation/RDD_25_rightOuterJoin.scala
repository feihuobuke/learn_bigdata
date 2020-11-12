package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:23
 * @Description: Performs an right outer join using two key-value RDDs.
 *              Please note that the keys must be generally comparable to make this work correctly.
 **/
object RDD_25_rightOuterJoin {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        val pairRDD1 = sc.parallelize(List(("cat", 2), ("cat", 5), ("book", 4), ("cat", 12)))
        val pairRDD2 = sc.parallelize(List(("cat", 2), ("cup", 5), ("mouse", 4), ("cat", 12)))
    
        val resultRDD: RDD[(String, (Option[Int], Int))] = pairRDD1.rightOuterJoin(pairRDD2)
    
        resultRDD.foreach(x => {
            println(x._1, x._2._1.getOrElse("null"), x._2._2)
        })
    }
}
