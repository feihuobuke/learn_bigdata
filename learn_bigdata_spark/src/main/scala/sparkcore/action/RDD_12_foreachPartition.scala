package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:29
 * @Description: Executes an parameterless function for each partition. Access to the data items
 *              contained in the partition is provided via the iterator argument.
 **/
object RDD_12_foreachPartition {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val names = List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee")
        val rdd1 = sc.parallelize(names, 3)
    
        /**
         * 这个 iter 就是一个分区的数据
         */
        rdd1.foreachPartition(iter => {
            
            // 这里面的所有代码都是在 task 里面执行的。如果如果打印记录每个 Task 的执行情况
            // 可以在此添加记录日志。
            println("----------------------------")
            for (x <- iter) {
                println(x)
            }
        })
    }
}
