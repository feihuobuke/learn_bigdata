package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:29
 * @Description: Executes an parameterless function for each data item.
 **/
object RDD_11_foreach {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val names = List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee")
        val rdd1 = sc.parallelize(names, 3)
        
        /**
         * foreach: 遍历
         */
        rdd1.foreach(x => {
            println("name => " + x)
        })
    }
}
