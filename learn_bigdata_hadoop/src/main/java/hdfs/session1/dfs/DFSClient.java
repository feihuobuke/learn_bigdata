package hdfs.session1.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: reiserx
 * Date:2020/7/1
 * Des:
 */
public class DFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs:hadoop102:9000"), conf, "reiser");

        fs.delete(new Path("/data"), false);
        //元数据管理
        fs.mkdirs(new Path("/data/new"));
        //写数据流程
//        fs.copyFromLocalFile();
    }
}
