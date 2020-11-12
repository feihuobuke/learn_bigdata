package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 10:58
 * @Description: Takes the RDD data of each partition and sends it via stdin to a shell-command.
 *              The resulting output of the command is captured and returned as a RDD of string values.
 **/
object RDD_31_pipe {
    
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setAppName("RDD_31_pipe").setMaster("local")
        val sc = new SparkContext(sparkConf)
    
    
        val dataRDD = sc.parallelize(1 to 9, 3)
    
        /*
         * TODO_MA Takes the RDD data of each partition and sends it via stdin to a shell-command.
         *  The resulting output of the command is captured and returned as a RDD of string values.
         */
        val resultRDD: RDD[String] = dataRDD.pipe("head -n 1")
        
        resultRDD.foreach(x => println(x))
    }
}
