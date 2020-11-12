package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 10:27
 * @Description: Extracts the keys from all contained tuples and returns them in a new RDD.
 **/
object RDD_06_keys {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = List((1, "sunli"), (2, "wuqilong"), (3, "huangxiaoming"), (3, "huanglei"))
        val rdd1: RDD[(Int, String)] = sc.parallelize(data1)
        
        /*
         * TODO_MA 收集一个key-value类型的RDD的key,组成一个新的RDD
         */
        val keysRDD: RDD[Int] = rdd1.keys
        
        keysRDD.foreach(x => println(x))
    }
}
