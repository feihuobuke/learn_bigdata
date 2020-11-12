package sparkstreaming.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;


/**
 * @author: reiserx
 * Date:2020/10/14
 * Des:SparkStreaming wordcount
 */
public class WordCountStreaming {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf();
        conf.setMaster("local[3]");
        conf.setAppName("wordcount");
        //初始化上下文
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(2));
        //获取数据
        JavaReceiverInputDStream<String> dataSteam = ssc.socketTextStream("localhost", 8888);
        
        //算子使用
        JavaDStream<String> wordsDstream = dataSteam.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDstream = wordsDstream.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCountDstream = pairDstream.reduceByKey(Integer::sum);

        //输出结果
        wordCountDstream.print();

        //启动任务
        ssc.start();
        ssc.awaitTermination();
        ssc.stop();

    }
}
