package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 12:56
 * @Description: Similar to zip. But provides more control over the zipping process.
 **/
object RDD_29_zipPartitions {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val data1 = Array[String]("huangbo", "xuzheng", "wangbaoqiang")
        val data2 = Array[Int](18, 19, 20)
        val data3 = Array[Int](28, 29, 30)
        
        val rdd1: RDD[String] = sc.parallelize(data1)
        val rdd2: RDD[Int] = sc.parallelize(data2)
        val rdd3: RDD[Int] = sc.parallelize(data3)
        
        def myfunc(aiter: Iterator[String], biter: Iterator[Int], citer: Iterator[Int]): Iterator[String] = {
            var res = List[String]()
            while (aiter.hasNext && biter.hasNext && citer.hasNext) {
                val x = aiter.next + " " + biter.next + " " + citer.next
                res ::= x
            }
            res.iterator
        }
        
        val resultRDDFunc: ((Iterator[String], Iterator[Int], Iterator[Int]) => Iterator[String]) =>
            RDD[String] =
            rdd1.zipPartitions(rdd2, rdd3)
        
        val resultRDD: RDD[String] = resultRDDFunc(myfunc)
        
        resultRDD.foreach(x => println(x))
    }
}
