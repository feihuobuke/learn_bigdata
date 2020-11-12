package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 作者： 马中华   https://blog.csdn.net/zhongqi2513
 * 时间： 2018/10/30 16:04
 * 需求：读取HDFS上的数据。插入到HBase库中
 * <p>
 * 程序运行之前，要先做两件事：
 * 1、把 students.txt文件放入： /bigdata/student/input/ 目录中
 * hadoop fs -mkdir -p /bigdata/student/input/
 * hadoop fs -put students.txt /bigdata/student/input/
 * 2、创建好一张hbase表： create "student", "info"
 * 3、把HDFS的 core-site.xml 和 hdfs-site.xml 文件导入到 resources 目录中
 */
public class HDFSDataToHBaseMR extends Configured implements Tool {

    public static final String HDFS_INPATH = "/bigdata/student/input";
    public static final String TABLE_NAME = "student";
    public static final String COLUMN_FAMILY_INFO = "info";

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new HDFSDataToHBaseMR(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] arg0) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);

        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Job job = Job.getInstance(configuration, "HDFSDataToHBaseMR");

        job.setJarByClass(HDFSDataToHBaseMR.class);

        job.setMapperClass(HBaseMR_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置数据的输出组件
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME, HBaseMR_Reducer.class, job);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path(HDFS_INPATH));

        boolean isDone = job.waitForCompletion(true);
        return isDone ? 0 : 1;
    }

    public static class HBaseMR_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class HBaseMR_Reducer extends TableReducer<Text, NullWritable, NullWritable> {

        protected void reduce(Text key, Iterable<NullWritable> values, Reducer.Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split(",");

            Put put = new Put(split[0].getBytes());
            put.addColumn(COLUMN_FAMILY_INFO.getBytes(), "name".getBytes(), split[1].getBytes());
            put.addColumn(COLUMN_FAMILY_INFO.getBytes(), "sex".getBytes(), split[2].getBytes());
            put.addColumn(COLUMN_FAMILY_INFO.getBytes(), "age".getBytes(), split[3].getBytes());
            put.addColumn(COLUMN_FAMILY_INFO.getBytes(), "department".getBytes(), split[4].getBytes());

            context.write(NullWritable.get(), put);
        }
    }
}
