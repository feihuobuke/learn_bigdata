package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:27
 * @Description: Looks for the very first data item of the RDD and returns it.
 **/
object RDD_05_first {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val rdd2 = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 3)
        
        def myfunc(index: Int, iter: Iterator[String]): Iterator[String] = {
            iter.map(x => "[partID:" + index + ", val: " + x + "]")
        }
        
        rdd2.mapPartitionsWithIndex(myfunc).foreach(println)
        
        val resultValue: String = rdd2.first()
        println(resultValue)
    }
}
