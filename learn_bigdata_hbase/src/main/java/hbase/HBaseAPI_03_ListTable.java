package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/9/11 10:43
 * Description：
 */
public class HBaseAPI_03_ListTable {

    public static void main(String[] args) throws IOException {
        // 请开始你的表演！
        Configuration config = HBaseConfiguration.create();
        config.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);
//        config.set("zookeeper.znode.parent", "/hbase225");

        // TODO_MA 注释：获取 connection 对象
        Connection connection = ConnectionFactory.createConnection(config);

        // TODO_MA 注释：获取 Admin 对象
        Admin admin = connection.getAdmin();

        // TODO_MA 注释：显示所有的表名
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }

        // TODO_MA 注释： 关闭
        admin.close();
        connection.close();
    }
}
