package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * 作者： 马中华   https://blog.csdn.net/zhongqi2513
 * 时间： 2018/10/30 16:04
 * 描述： 编写mapreduce程序从hbase读取数据，然后存储到hdfs
 */
public class HBaseDataToHDFSMR {

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
		configuration.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);
		configuration.set(HBase_Constants.HDFS_CONNECT_KEY, HBase_Constants.HDFS_CONNECT_VALUE);
        System.setProperty("HADOOP_USER_NAME", "bigdata");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(HBaseDataToHDFSMR.class);

        // 输入数据来源于hbase的user_info表
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob("student", scan, HBaseDataToHDFSMRMapper.class, Text.class, NullWritable.class, job);

        //	RecordReader  --- TableRecordReader
        //	InputFormat ----- TextInputFormat

        // 数据输出到hdfs
        FileOutputFormat.setOutputPath(job, new Path("/bigdata/hbase2hdfs/output1"));

        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);
    }


    /**
     * mapper的输入key-value类型是：ImmutableBytesWritable, Result
     * mapper的输出key-value类型就可以由用户自己制定
     */
    public static class HBaseDataToHDFSMRMapper extends TableMapper<Text, NullWritable> {
        /**
         * keyType: LongWritable -- ImmutableBytesWritable:rowkey
         * ValueType: Text --  Result:hbase表中某一个rowkey查询出来的所有的key-value对
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {

            String rowkey = Bytes.toString(key.copyBytes());
            List<Cell> listCells = value.listCells();
            Text text = new Text();

            for(Cell cell : listCells) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String v = new String(CellUtil.cloneValue(cell));
                long ts = cell.getTimestamp();

                text.set(rowkey + "\t" + family + "\t" + qualifier + "\t" + v + "\t" + ts);
                context.write(text, NullWritable.get());
            }
        }
    }
}

