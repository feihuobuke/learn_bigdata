package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 14:37
 * @Description: Computes the same thing as aggregate, except it aggregates the elements of the RDD
 *              in a multi-level tree pattern. Another difference is that it does not use the initial value
 *              for the second reduce function (combOp).
 *              By default a tree of depth 2 is used, but this can be changed via the depth parameter.
 **/
object RDD_28_treeAggregate {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data = List(1, 2, 3, 4, 5)
        val dataRDD: RDD[Int] = sc.parallelize(data, 3)
        
        def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
            iter.map(x => "[partID:" + index + ", val: " + x + "]")
        }
        
        val resultValues: Array[String] = dataRDD.mapPartitionsWithIndex(myfunc).collect
        for (elem <- resultValues) {
            println(elem)
        }
        
        /*
         * TODO_MA 先求每个分区的最大值，然后求每个分区的总和
         */
        val resultValue: Int = dataRDD.treeAggregate(0)(math.max(_, _), _ + _)
        
        println(resultValue)
    }
}
