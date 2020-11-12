package wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2018年04月22日 上午9:27:17
 * <p>
 * 描述： Java版本的WordCount -- 使用lambda表达式
 */
public class WordCount_Java8 {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage WordCountJavaLambda<input><output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(WordCount_Java8.class.getSimpleName());

        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取数据
        JavaRDD<String> jrdd = jsc.textFile(args[0]);
        //切割压平
        JavaRDD<String> jrdd2 = jrdd.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
        //和1组合
        JavaPairRDD<String, Integer> jprdd = jrdd2.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
        //分组聚合
        JavaPairRDD<String, Integer> res = jprdd.reduceByKey((a, b) -> a + b);
        //保存
        res.saveAsTextFile(args[1]);
        //释放资源
        jsc.close();
    }
}