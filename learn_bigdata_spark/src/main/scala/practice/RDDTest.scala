package practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {

  /**
   * 算子 reduce
   *
   * @param sc
   */
  def testReduce(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))
    //reduce聚合
    val result = rdd1.reduce(_ + _)
    println(result)
  }


  def testJoin(sc: SparkContext): Unit = {

    val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
    val list2 = List((1, 99), (2, 98), (3, 97))

    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    val joinRDD = rdd1.join(rdd2)
    joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))

  }


  def testReduceBykey(sc: SparkContext): Unit = {
    val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
    val mapRDD = sc.parallelize(list)
    val rdd2 = mapRDD.reduceByKey(_ + _)
    rdd2.foreach(t => println("门派: " + t._1 + "---->" + t._2))

  }

  def main(args: Array[String]): Unit = {
    // 初始化编程入口
//    val conf = new SparkConf().setMaster("local").setAppName("rdd-practice")
    val conf = new SparkConf().setMaster("spark://reiser001:7077").setAppName("rdd-practice")
    val sc = new SparkContext(conf)
    //    testMap(sc)
    //    testFlatmap(sc)
    //    testUnion(sc)
    //    testReduce(sc)
    //    testJoin(sc)
    testReduceBykey(sc)
//    val a = 3
//    sc.broadcast(a)
//
//    sc.longAccumulator("acc")

  }


  /**
   * 算子 unin
   *
   * @param sc
   */
  private def testUnion(sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))

    //并集
    val rdd3 = rdd1.union(rdd2)
    //交集
    val rdd4 = rdd1.intersection(rdd2)
    //差集
    val rdd5 = rdd1.subtract(rdd2)

    //    printIntArray(rdd3.collect())//5,6,4,3,1,2,3,4,
    //去重
    rdd3.distinct().foreach(println) //4,6,2,1,3,5,
    //去重 + 排序
    rdd3.distinct().sortBy(x => x, ascending = true).foreach(println)
    rdd4.foreach(println)
    rdd5.foreach(println)
  }

  /**
   * 算子 map sortBy filter
   *
   * @param sc
   */
  private def testMap(sc: SparkContext) = {
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10))
    val rdd2 = rdd1.map(_ * 2).sortBy(x => x, ascending = true)
    val rdd3 = rdd2.filter(_ >= 10)

    rdd3.foreach(println)
  }


  /**
   * 算子 flatmap
   *
   * @param sc
   */
  private def testFlatmap(sc: SparkContext) = {
    val rdd11 = sc.parallelize(Array("a b c", "d e f", "h i j"))
    val rdd12 = rdd11.flatMap(_.split(" "))


    rdd12.foreach(println)
  }
}
