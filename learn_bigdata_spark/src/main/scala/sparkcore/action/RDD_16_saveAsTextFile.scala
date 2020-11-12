package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:31
 * @Description: Saves the RDD as text files. One line at a time.
 **/
object RDD_16_saveAsTextFile {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        /*
         * TODO_MA 普通的单值RDD，只能使用 saveAsTextFile() 和 saveAsObjectFile()
         */
        val data1 = List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat")
        val rdd1 = sc.parallelize(data1, 3)
        rdd1.saveAsTextFile("")
    
        /**
         * TODO_MA key-value类型的RDD
         */
        val data2 = List(("a", 1), ("a", 3), ("b", 2),  ("b", 1))
        val rdd2 = sc.parallelize(data2)
        rdd2.saveAsTextFile("")
    }
}
