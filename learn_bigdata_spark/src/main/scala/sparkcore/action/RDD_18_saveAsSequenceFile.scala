package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:31
 * @Description: Saves the RDD as a Hadoop sequence file.
 **/
object RDD_18_saveAsSequenceFile {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        /*
         * TODO_MA key-value类型的RDD保存为 SequenceFile 的格式文件
         */
        val data2 = List(("a", 1), ("a", 3), ("b", 2),  ("b", 1))
        val rdd2 = sc.parallelize(data2)
        rdd2.saveAsSequenceFile("")
    }
}
