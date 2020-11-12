package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:29
 * @Description: Very similar to count, but counts the values of a RDD consisting of two-component tuples
 *              for each distinct key separately.
 **/
object RDD_10_countByKey {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog"))
        val rdd = sc.parallelize(data, 2)
        
        /**
         * 统计每个key出现的次数
         */
        val result: collection.Map[Int, Long] = rdd.countByKey()
        
        for (a <- result) {
            println(a._1, a._2)
        }
    }
}
