package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 11:45
 * @Description:    keyBy的作用：
 *                  给rdd中的每个value，生成一个key，最后变成 key-value 形式
 *                  RDD[value] ==> RDD[(key, value)]
 *
 * Constructs two-component tuples (key-value pairs) by applying a function on each data item.
 * The result of the function becomes the key and the original data item
 * becomes the value of the newly created tuples.
 **/
object RDD_06_keyBy {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        /*
         * TODO_MA 第一个案例
         */
        val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
        val resultRDD: RDD[(Int, String)] = rdd1.keyBy(_.length)
        resultRDD.foreach(println)
    }
}
