package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/9/11 10:43
 * Description：
 */
public class HBaseAPI_04_ListTableDescriptor {

    public static void main(String[] args) throws IOException {
        // 请开始你的表演！
        Configuration config = HBaseConfiguration.create();
        config.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);

        // TODO_MA 注释：获取 connection 对象
        Connection connection = ConnectionFactory.createConnection(config);

        // TODO_MA 注释：获取 Admin 对象
        Admin admin = connection.getAdmin();

        // TODO_MA 注释：显示所有的表名
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for(TableDescriptor td : tableDescriptors) {

            // 表名
            TableName tableName = td.getTableName();
            System.out.println("表名：" + Bytes.toString(tableName.getName()));
            // 列簇
            ColumnFamilyDescriptor[] columnFamilies = td.getColumnFamilies();

            System.out.print("列簇为：");
            for(ColumnFamilyDescriptor cfd : columnFamilies) {
                System.out.print(Bytes.toString(cfd.getName()) + "\t");
            }
            System.out.println("\n---------------------------------------");
        }

        // TODO_MA 注释： 关闭
        admin.close();
        connection.close();
    }
}
