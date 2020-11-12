package mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static mapreduce.sort.MyPartitoner.getPartitionId;

/**
 * @author: reiserx
 * Date:2020/8/16
 * Des:
 */
public class Sort {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();


        Job job = Job.getInstance(configuration, "sort number");

        job.setJarByClass(Sort.class);
        //设置 map 和 redeuce 的类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
//        job.setPartitionerClass(MyPartitoner.class);

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);


        // 4 设置map输出
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
//        job.setPartitionerClass(HashPartitioner);

        job.setNumReduceTasks(4);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class MyMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int p = getPartitionId(Long.parseLong(value.toString()), context.getNumReduceTasks());
            context.write(new LongWritable(p), new LongWritable(1));
        }
    }

    // 如果有多个 reducer 呢
    private static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        long count;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable item : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }

    }

}
