package sparkcore.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 11:02
 * @Description: Randomly splits an RDD into multiple smaller RDDs according to a weights Array
 *              which specifies the percentage of the total data elements that is assigned to each smaller RDD.
 *              Note the actual size of each smaller RDD is only approximately equal to the percentages
 *              specified by the weights Array.
 *              The second example below shows the number of items in each smaller RDD does not exactly
 *              match the weights Array.
 *              A random optional seed can be specified.
 *              This function is useful for spliting data into a training set and a testing set for
 *              machine learning.
 **/
object RDD_32_randomSplit {
    
    def main(args: Array[String]): Unit = {
        
        val sparkConf = new SparkConf().setAppName("RDD_31_pipe").setMaster("local")
        val sc = new SparkContext(sparkConf)
        
        val dataRDD: RDD[Int] = sc.parallelize(1 to 10)
        
        /*
         * TODO_MA 按照百分比随机数据到多个RDD
         */
        val splitsRDD: Array[RDD[Int]] = dataRDD.randomSplit(Array(0.6, 0.4))
        
        splitsRDD.foreach(subRDD => {
            subRDD.foreach(x => print(x + "\t"))
            println()
        })
        
    }
}
