package wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *   注释： 编程套路
 *   1、获取编程入口                    环境对象 链接对象
 *   2、通过编程入口获取数据抽象         Source 对象
 *   3、针对数据抽象对象执行各种计算     Action Transformation
 *   4、提交任务运行                  Submit
 *   5、输出结果                     Sink
 *   6、回收资源                      stop close
 *
 *   在使用scala编写的时候： 链式调用
 *     sparkContext.textFile(func1).flatMap(func2).xxx()
 *     不要这么写！  代码规范：可读性，可维护性，可扩展性
 *          rdd1 = sparkContext.textFile(func1)
 *          rdd2 = rdd1..flatMap(func2)
 *          rdd3 = rdd2.xxxx()
 *
 *   在使用java编写的时候：
 *     sparkContext.textFile(“path”).flatMap(new FlatMapFunctoin(){
 *         @Override
 *             public Iterator<String> call(String s) throws Exception {
 *                 return Arrays.asList(s.split(" ")).iterator();
 *             }
 *     })
 */
public class WordCount_Java7 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取数据
        JavaRDD<String> linesRDD = sc.textFile("file:///c:/words.txt", 1);

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释：
         *   1、flatMap 当中的参数就是指定逻辑！
         *   2、逻辑不是一个函数，而是一个匿名对象
         *   3、linesRDD 是一个数据集合
         *   4、把 linesRDD 中的每个元数据，拿出来作为参数，给  call 执行一次。
         *   5、每次调用得到的结果，就拼接成为一个新的集合
         */
        // 获得单词RDD
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 获得每个单词一次的RDD
        JavaPairRDD<String, Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 获得每个单词，统计出出现了多少次的RDD
        JavaPairRDD<String, Integer> wordAndCount = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 简单打印输出
        wordAndCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1()+ "\t" +stringIntegerTuple2._2());
            }
        });


        // 如果要进行排序
        JavaPairRDD<Integer, String> wordAndOneSort = wordAndCount.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        wordAndOneSort = wordAndOneSort.sortByKey(false);

        wordAndCount = wordAndOneSort.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        wordAndCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1()+ "\t" +stringIntegerTuple2._2());
            }
        });

        sc.close();
    }
}
