package mapreduce.sort;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static mapreduce.sort.MyPartitoner.getPartitionId;

/**
 * @author: reiserx
 * Date:2020/8/16
 * Des:
 */
public class Sort2 {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();


        Job job = Job.getInstance(configuration, "sort number 2");

        job.setJarByClass(Sort2.class);
        //设置 map 和 redeuce 的类
        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReducer2.class);
        job.setPartitionerClass(MyPartitoner.class);

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

        for (int i = 2; i < args.length; i++) {
            job.addCacheFile(new URI(args[i]));
        }

        job.setNumReduceTasks(4);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    private static class MyMapper2 extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        Map<String, Long> sizes = new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();

            long count = 0;
            for (URI path : cacheFiles) {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.getPath()), "UTF-8"));

                FileSystem fs = FileSystem.get(context.getConfiguration());
                FSDataInputStream in_file = fs.open(new Path(path.getPath()));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in_file));//each line of reference file

                String line;
                while (StringUtils.isNotEmpty(line = reader.readLine())) {
                    // 2 切割
                    String[] fields = line.split("\t");
                    long v = Long.parseLong(fields[1]);
                    // 3 缓存数据到集合
                    sizes.put(fields[0], count);
                    count = count + v;
                }

                // 4 关流
                reader.close();
            }

        }

        //            String[] paths = {
//                    "/Users/reiserx/code/nx-bigdata/hadoop/src/main/resources/sort/part-r-00000",
//                    "/Users/reiserx/code/nx-bigdata/hadoop/src/main/resources/sort/part-r-00001",
//                    "/Users/reiserx/code/nx-bigdata/hadoop/src/main/resources/sort/part-r-00002",
//                    "/Users/reiserx/code/nx-bigdata/hadoop/src/main/resources/sort/part-r-00003"
//            };

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            long v = Long.parseLong(value.toString());
            int p = getPartitionId(v, context.getNumReduceTasks());
            String pStr = String.valueOf(p);
            if (sizes.containsKey(pStr)) {
                long size = sizes.get(pStr);
                context.write(new LongWritable(v), new LongWritable(size));
            }

        }

    }

    // 如果有多个 reducer 呢
    private static class MyReducer2 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        long index = -1;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.getNumReduceTasks();

            for (LongWritable item : values) {
                if (index == -1) {
                    index = item.get();
                }
                index++;
                context.write(new LongWritable(index), key);
            }
        }

    }
}

