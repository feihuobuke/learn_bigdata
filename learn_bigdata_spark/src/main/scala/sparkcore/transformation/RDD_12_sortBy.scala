package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:47
 * @Description: This function sorts the input RDD's data and stores it in a new RDD.
 *              The first parameter requires you to specify a function which  maps the input data
 *              into the key that you want to sortBy. The second parameter (optional) specifies
 *              whether you want the data to be sorted in ascending or descending order.
 **/
object RDD_12_sortBy {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
        val dataRDD: RDD[(Int, String)] = sc.parallelize(list)
        
        /**
         * TODO_MA 指定排序规则
         */
        val resultRDD: RDD[(Int, String)] = dataRDD.sortBy(x => x._1, false)
        
        resultRDD.foreach(x => println(x))
    }
}
