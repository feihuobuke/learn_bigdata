package sparkcore.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 18:03
 * @Description: Returns an RDD containing only the items in the key range specified. From our testing,
 *              it appears this only works if your data is in key value pairs and it has already been sorted by
 *              key.
 **/
object RDD_05_filterByRange {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        /*
         * TODO_MA filterByRange只能作用于key-value类型的RDD，按照key的范围来过滤数据
         */
        val data = List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater"))
        val randRDD = sc.parallelize(data, 3)
        
        val resultRDD = randRDD.filterByRange(3, 5)
        resultRDD.foreach(println)
    }
}
