package mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/8/16
 * Des:
 */
public class SortByCounter {

    public enum MyCouterWordCount {
        COUNT_100, COUNT_10000, COUNT_OTHER
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "sort number");

        // 2 设置jar加载路径
        job.setJarByClass(SortByCounter.class);

        //设置 map 和 redeuce 的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setPartitionerClass(MyPartitoner.class);
        job.setCombinerClass(MyCombiner.class);

        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);


        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }


        // 4 设置map输出
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
//        job.setPartitionerClass(HashPartitioner);

        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class MyCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        Counter counter100;
        Counter counter10000;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            counter100 = context.getCounter(MyCouterWordCount.COUNT_100);
            counter10000 = context.getCounter(MyCouterWordCount.COUNT_10000);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable item : values) {
                long v = key.get();
                // 获取计数器，行数计数器
                if (v < 100) {
                    context.write(key, new LongWritable(0));
                } else if (v < 10000) {
                    context.write(key, new LongWritable(counter100.getValue()));
                } else {
                    context.write(key, new LongWritable(counter100.getValue() + counter10000.getValue()));
                }

            }

        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        Counter counter100;
        Counter counter10000;
        Counter counterOther;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            counter100 = context.getCounter(MyCouterWordCount.COUNT_100);
            counter10000 = context.getCounter(MyCouterWordCount.COUNT_10000);
            counterOther = context.getCounter(MyCouterWordCount.COUNT_OTHER);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int v = Integer.parseInt(value.toString());
            // 获取计数器，行数计数器
            if (v < 100) {
                counter100.increment(1);
            } else if (v < 10000) {
                counter10000.increment(1);
            } else {
                counterOther.increment(1);
            }
            context.write(new LongWritable(v), new LongWritable(1));
        }
    }

    public static class MyPartitoner extends Partitioner<LongWritable, LongWritable> {

        @Override
        public int getPartition(LongWritable key, LongWritable value, int numPartitions) {

            long v = key.get();
            if (v < 100) {
                return 0;
            } else if (v < 10000) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    public static class WCReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        long index;
        LongWritable in = new LongWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            for (LongWritable v : values) {
                if (index == 0) {
                    index = v.get();
                }
                index++;
                in.set(index);
                context.write(in, key);
            }


        }
    }
}


