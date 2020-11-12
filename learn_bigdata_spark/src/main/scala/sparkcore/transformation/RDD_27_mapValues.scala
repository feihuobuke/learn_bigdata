package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 18:11
 * @Description: Takes the values of a RDD that consists of two-component tuples,
 *              and applies the provided function to transform each value.
 *              Then, it forms new two-component tuples
 *              using the key and the transformed value and stores them in a new RDD.
 **/
object RDD_27_mapValues {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        val data1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
        val rdd1 = data1.map(x => (x.length, x))
    
        /*
         * TODO_MA 针对key-value类型的RDD，只对value进行map操作
         */
        val resultRDD: RDD[(Int, String)] = rdd1.mapValues("x" + _ + "x")
        
        resultRDD.foreach(println)
    }
}
