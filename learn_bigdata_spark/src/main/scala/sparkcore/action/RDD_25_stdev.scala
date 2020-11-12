package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 14:32
 * @Description: Calls stats and extracts either stdev-component or corrected sampleStdev-component.
 **/
object RDD_25_stdev {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List(1, 2, 3, 4, 5)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
        
        /*
         * TODO_MA Compute the population standard deviation of this RDD's elements.
         *      计算标准差
         */
        val resultValue: Double = dataRDD.stdev()
        
        println(resultValue)
    }
}
