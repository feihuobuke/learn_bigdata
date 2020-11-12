package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 14:35
 * @Description: Works like reduce except reduces the elements of the RDD in a multi-level tree pattern.
 **/
object RDD_27_treeReduce {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List(1, 2, 3, 4, 5)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
        
        val resultValue: Int = dataRDD.treeReduce(_ + _)
        
        println(resultValue)
    }
}
