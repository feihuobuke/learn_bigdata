package sparkcore.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:47
 * @Description: This function sorts the input RDD's data and stores it in a new RDD. The output RDD is a
 *              shuffled RDD because it stores data that is output by a reducer which has been shuffled. The
 *              implementation of this function is actually very clever. First, it uses a range partitioner to
 *              partition the data in ranges within the shuffled RDD. Then it sorts these ranges individually
 *              with mapPartitions using standard sort mechanisms.
 **/
object RDD_12_sortByKey {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        sortByKey(sc)
    }
    
    def sortByKey(sc: SparkContext): Unit = {
        val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
        sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
    }
}
