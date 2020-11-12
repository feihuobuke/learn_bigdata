package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 11:35
 * @Description: Randomly samples the key value pair RDD according to the fraction of each key
 *              you want to appear in the final RDD.
 **/
object RDD_20_sampleByKey {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2))
        val pairRDD1 = sc.parallelize(data1, 2)
    
        /*
         * TODO_MA 最终抽样的概念是按照 指定的key的概率。
         */
        val sampleFraction: Map[String, Double] = List(("cat", 0.25), ("mouse", 0.5), ("dog", 0.25)).toMap
        val resultRDD: RDD[(String, Int)] = pairRDD1.sampleByKey(false, sampleFraction)
        
        resultRDD.foreach(x => println(x))
    }
}
