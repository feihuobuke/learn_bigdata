package mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: reiserx
 * Date:2020/8/25
 * Des:
 */
public class MyPartitoner extends Partitioner<LongWritable, LongWritable> {
    public static int getPartitionId(long v, int numPartitions) {
        if (numPartitions <= 0) {
            return 0;
        }
        for (int i = 1; i < numPartitions; i++) {
            if (v < Math.pow(10, i)) {
                return i - 1;
            }
        }
        return numPartitions - 1;
    }

    @Override
    public int getPartition(LongWritable key, LongWritable value, int numPartitions) {


        long v = key.get();

        return getPartitionId(v, numPartitions);
    }
}
