package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:26
 * @Description:
 **/
object RDD_02_reduceByKeyLocal {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
//        sc.textFile()
        
        val data = List(("a", 1), ("a", 3), ("b", 2), ("b", 1))
        val rdd1 = sc.parallelize(data)
        
        val resultValue: collection.Map[String, Int] = rdd1.reduceByKeyLocally(_ + _)
        for (t <- resultValue) {
            println(t._1, t._2)
        }
    }
}
