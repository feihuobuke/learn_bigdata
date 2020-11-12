package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/9/11 11:01
 * Description：
 */
public class HBaseAPI_05_CRUD {

    public static final String TABLE_NAME = "blog";
    public static final String CF_BASE = "article";
    public static final String CF_EXTRA = "author";

    public static final byte[] ADD_FLAG = new byte[]{0x00};

    public static final String COLUMN_NAME = "name";

    public static Configuration conf = null;
    public static HBaseAdmin admin = null;
    public static HTable table = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);

        try {
            Connection connect = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connect.getAdmin();
            table = (HTable) connect.getTable(TableName.valueOf(TABLE_NAME));
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        // 请开始你的表演！
        String[] family = {CF_BASE, CF_EXTRA};

        // TODO_MA 注释：创建表
        creatTable(TABLE_NAME, family);


        // TODO_MA 注释：添加数据
        String[] column1 = {"title", "content", "tag"};
        String[] value1 = {"Head First HBase", "HBase is the Hadoop database", "Hadoop,HBase,NoSQL"};
        String[] column2 = {"name", "nickname"};
        String[] value2 = {"nicholas", "lee"};
        String[] value3 = {"lilaoshi", "malaoshi"};
        addData("rowkey1", "blog", column1, value1, column2, value2);
        addData("rowkey1", "blog", column1, value1, column2, value3);
        addData("rowkey2", "blog", column1, value1, column2, value2);
        addData("rowkey3", "blog", column1, value1, column2, value2);

        // TODO_MA 注释：遍历查询， 根据row key范围遍历查询
        getResultScann("blog", "rowkey2", "rowkey3");

        // TODO_MA 注释： 查询
        getResult("blog", "rowkey1");

        // TODO_MA 注释：查询某一列的值
        getResultByColumn("blog", "rowkey1", "author", "name");

        // TODO_MA 注释：更新列
        updateTable("blog", "rowkey1", "author", "name", "bin");

        // TODO_MA 注释：查询某一列的值
        getResultByColumn("blog", "rowkey1", "author", "name");

        // TODO_MA 注释：查询某列的多版本
        getResultByVersion("blog", "rowkey1", "author", "name");

        // TODO_MA 注释：删除一列
        deleteColumn("blog", "rowkey1", "author", "nickname");

        // TODO_MA 注释：删除所有列
        deleteRowkey("blog", "rowkey1");

        // TODO_MA 注释：删除表
        deleteTable(TABLE_NAME);
    }

    public static void creatTable(String tableName, String[] familys) throws Exception {

        // TODO_MA 注释：表名
        TableName tableNameObj = TableName.valueOf(tableName);

        // TODO_MA 注释： 构造列簇
        List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<ColumnFamilyDescriptor>();
        for(String family : familys) {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).setDataBlockEncoding(DataBlockEncoding.PREFIX)
                    .setBloomFilterType(BloomType.ROW).build();
            familyDescriptors.add(cf);
        }

        // TODO_MA 注释：构造表结构
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableNameObj).setColumnFamilies(familyDescriptors).build();

        if(admin.tableExists(tableNameObj)) {
            System.out.println("Table【" + tableName + "】Exists！");
            System.exit(0);
        } else {
            admin.createTable(tableDescriptor);
            System.out.println("Table Created {表名: " + tableName + ", 列簇: [" + StringUtils.join(",", familys) + "]");
        }
    }

    /**
     * 为表添加数据（适合知道有多少列簇的固定表）
     *
     * @rowKey rowKey
     * @tableName 表名
     * @column1 第一个列簇列表
     * @value1 第一个列的值的列表
     * @column2 第二个列簇列表
     * @value2 第二个列的值的列表
     */
    public static void addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2,
            String[] value2) throws IOException {
        // 设置rowkey
        Put put = new Put(Bytes.toBytes(rowKey));

        // 获取所有的列簇
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for(int i = 0; i < columnFamilies.length; i++) {
            // 获取列簇名
            String familyName = columnFamilies[i].getNameAsString();
            // article列簇put数据
            if(familyName.equals("article")) {
                for(int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            // author列簇put数据
            if(familyName.equals("author")) {
                for(int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    /**
     * 根据rwokey查询
     *
     * @rowKey rowKey
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        HBasePrintUtil.printResult(result);
        return result;
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public static void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            HBasePrintUtil.printResultScanner(rs);
        } finally {
            rs.close();
        }
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     * 切记：包括下界，不包括上界
     */
    public static void getResultScann(String tableName, String start_rowkey, String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        try {
            rs = table.getScanner(scan);
            HBasePrintUtil.printResultScanner(rs);
        } finally {
            rs.close();
        }
    }

    /**
     * 查询表中的某一列
     *
     * @tableName 表名
     * @rowKey rowKey
     */
    public static void getResultByColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        // 获取指定列簇和列修饰符对应的列
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        Result result = table.get(get);
        HBasePrintUtil.printResult(result);
    }

    /**
     * 更新表中的某一列
     *
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列簇名
     * @columnName 列名
     * @value 更新后的值
     */
    public static void updateTable(String tableName, String rowKey, String familyName, String columnName, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /**
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列簇名
     * @columnName 列名
     */
    public static void getResultByVersion(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        HBasePrintUtil.printResult(result);
    }

    /**
     * 删除指定的列
     *
     * @tableName 表名
     * @rowKey rowKey
     * @familyName 列簇名
     * @columnName 列名
     */
    public static void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws IOException {
        // TODO_MA 注释：表名
        TableName tableNameObj = TableName.valueOf(tableName);
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + " is deleted!");
    }

    /**
     * 删除指定的列
     *
     * @tableName 表名
     * @rowKey rowKey
     */
    public static void deleteRowkey(String tableName, String rowKey) throws IOException {
        Delete deleteOneRowkey = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteOneRowkey);
        System.out.println("One Rowkey has deleted!");
    }

    public static void deleteRowkey(String rowKey) throws IOException {
        Delete deleteOneRowkey = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteOneRowkey);
        System.out.println("One Rowkey has deleted!");
    }

    /**
     * TODO_MA 删除表
     *
     * @tableName 表名
     */
    public static void deleteTable(String tableName) throws IOException {
        // TODO_MA 注释：表名
        TableName tableNameObj = TableName.valueOf(tableName);

        admin.disableTable(tableNameObj);
        admin.deleteTable(tableNameObj);
        System.out.println(tableName + " is deleted!");
    }
}
