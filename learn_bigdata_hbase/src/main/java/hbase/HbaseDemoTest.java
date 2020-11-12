package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/9/13
 * Des:
 */
public class HbaseDemoTest {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);
        //获取 connection 对象
        Connection connection = ConnectionFactory.createConnection(config);

        //获取 Admin 对象
        Admin admin = connection.getAdmin();

        TableName studentTable = TableName.valueOf("student");
        if (admin.tableExists(studentTable)) {
            System.out.println("表已经存在");
        } else {

            // TODO 注释：构造列簇
            ColumnFamilyDescriptor cf1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1"))
                    .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                    .setBloomFilterType(BloomType.ROW)
                    .build();

            // TODO 注释：构建表对象
            TableDescriptor table = TableDescriptorBuilder.newBuilder(studentTable)
                    .setColumnFamily(cf1)
                    .build();

            admin.createTable(table);

            if(admin.tableExists(studentTable)){
                System.out.println("student 表创建成功");
            }else{
                System.out.println("创建表失败");
            }

        }
    }
}
