package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:26
 * @Description: Converts the RDD into a Scala array and returns it. If you provide a standard
 *              map-function (i.e. f = T -> U) it will be applied before inserting the values
 *              into the result array.
 **/
object RDD_03_collect {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val rdd = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
        
        val resultRDD: Array[String] = rdd.collect()
        
        println(resultRDD.mkString(","))
    }
}
