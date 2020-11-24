package com.reiser.sparktop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * @author: reiserx
 * Date:2020/11/24
 * Des:
 */
public class CategoryTop {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("My App");
        JavaSparkContext jsc = new JavaSparkContext(config);
        JavaRDD<String> sourceRDD = jsc.textFile(args[0]);
        JavaRDD<Log> logRDD = sourceRDD.flatMap((FlatMapFunction<String, Log>) s -> {
            List<Log> result = new ArrayList<>();
            String[] values = s.split(",");
            String click_category_ids = values[7];
            String order_category_ids = values[9];
            String pay_category_ids = values[11];

            result.add(new Log(Long.valueOf(click_category_ids), 1L, 0L, 0L));

            for (String orderId : order_category_ids.split("\\^A")) {
                result.add(new Log(Long.valueOf(orderId), 0L, 1L, 0L));
            }
            for (String payId : pay_category_ids.split("\\^A")) {
                result.add(new Log(Long.valueOf(payId), 0L, 0L, 1L));
            }
            return result.iterator();
        });
        JavaPairRDD<Long, Log> logByKeyRDD =
                logRDD.mapToPair((PairFunction<Log, Long, Log>) tuple4 -> Tuple2.apply(tuple4.getCategoryId(), tuple4));

        JavaPairRDD<Long, Log> logCountByKeyRDD = logByKeyRDD
                .reduceByKey((Function2<Log, Log, Log>) (v1, v2) ->
                        new Log(v1.getCategoryId(),
                                v1.getClickCount() + v2.getClickCount(),
                                v1.getOrderCount() + v2.getOrderCount(),
                                v1.getPayCount() + v2.getPayCount()));

        JavaRDD<Log> LogWithCountRDD = logCountByKeyRDD.map((Function<Tuple2<Long, Log>, Log>) v1 -> v1._2);
        JavaRDD<Log> sortRDD = LogWithCountRDD.sortBy((Function<Log, SecondarySort>) SecondarySort::new, false, 1);
        for (Log log : sortRDD.top(10)) {
            System.out.println(log);
        }
        jsc.close();
    }

    public static class SecondarySort implements Comparable<SecondarySort>, Serializable {
        Long payCount;
        Long orderCount;
        Long clickCount;

        public SecondarySort(Log log) {
            this.clickCount = log.getClickCount();
            this.orderCount = log.getOrderCount();
            this.payCount = log.getPayCount();
        }

        @Override
        public int compareTo(SecondarySort v2) {
            if (!this.clickCount.equals(v2.clickCount)) {
                return (int) (clickCount - v2.clickCount);
            }
            if (!this.orderCount.equals(v2.orderCount)) {
                return (int) (orderCount - v2.orderCount);
            }
            if (!this.payCount.equals(v2.payCount)) {
                return (int) (payCount - v2.payCount);
            }
            return 0;
        }
    }
}
