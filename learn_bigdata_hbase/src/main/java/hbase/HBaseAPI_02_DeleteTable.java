package hbase;

//import com.mazh.hbase.core.util.HBase_Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/9/8 10:45
 * Description：
 */
public class HBaseAPI_02_DeleteTable {

    public static void main(String[] args) throws IOException {
        // 请开始你的表演！
        Configuration config = HBaseConfiguration.create();
        config.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);

        // TODO_MA 注释：获取 connection 对象
        Connection connection = ConnectionFactory.createConnection(config);

        // TODO_MA 注释：获取 Admin 对象
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf("student");

        // TODO_MA 注释：判断表是否存在，如果存在，则先disable，然后delete
        if(admin.tableExists(tableName)) {
            //先disable再delete
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("table " + tableName.getNameAsString() + " delete success");
        } else {
            System.out.println("table " + tableName.getNameAsString() + " is not exists");
        }
    }
}
