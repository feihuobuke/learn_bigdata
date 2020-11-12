package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:30
 * @Description: Aggregates the values of each partition.
 *              The aggregation variable within each partition is initialized with zeroValue.
 **/
object RDD_13_fold {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
        val result: Int = rdd.fold(10)(_ + _)
        
        println(result)
    }
}
