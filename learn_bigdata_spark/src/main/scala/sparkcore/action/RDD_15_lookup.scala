package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:31
 * @Description: Scans the RDD for all keys that match the provided value and
 *              returns their values as a Scala sequence.
 **/
object RDD_15_lookup {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
        val rdd1 = data1.map(x => (x.length, x))
        
        // TODO_MA Scans the RDD for all keys that match the provided value
        //  and returns their values as a Scala sequence.
        // TODO_MA 有点类似于按照key找value
        val resultValues: Seq[String] = rdd1.lookup(5)
        
        resultValues.foreach(println)
    }
}
