package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2020/9/13
 * Des:
 */
public class HBaseDemoImpl implements HBaseDemoInterface {
    static Configuration config = null;
    static Connection connection = null;
    static Admin admin = null;

    static {
        config = HBaseConfiguration.create();
        config.set(HBase_Constants.ZK_CONNECT_KEY, HBase_Constants.ZK_CONNECT_VALUE);
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void close() throws IOException {
        admin.close();
        connection.close();
    }


    @Override
    public void getAllTables() throws Exception {
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }


    }

    @Override
    public void createTable(String tableName, String[] family) throws Exception {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            System.out.println("表已经存在");
        } else {
            // TODO 注释：构建列蔟对象
            List<ColumnFamilyDescriptor> list = new ArrayList<>();
            for (String s : family) {
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(s))
                        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                        .setBloomFilterType(BloomType.ROW)
                        .build();
                list.add(cf);
            }


            // TODO 注释：构建表对象
            TableDescriptor table = TableDescriptorBuilder.newBuilder(name)
                    .setColumnFamilies(list)
                    .build();

            admin.createTable(table);

            if (admin.tableExists(name)) {
                System.out.println("表创建成功");
            } else {
                System.out.println("表创建失败");
            }
        }

    }

    @Override
    public void createTable(TableDescriptor tds) throws Exception {
        admin.createTable(tds);
    }

    @Override
    public void createTable(String tableName, TableDescriptor tds) throws Exception {
//        tds.getTableName();

    }

    @Override
    public void descTable(String tableName) throws Exception {
        admin.getDescriptor(TableName.valueOf(tableName));
    }

    @Override
    public boolean existTable(String tableName) throws Exception {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    @Override
    public void disableTable(String tableName) throws Exception {
        admin.disableTable(TableName.valueOf(tableName));
    }

    @Override
    public void dropTable(String tableName) throws Exception {
        admin.deleteTable(TableName.valueOf(tableName));
    }

    @Override
    public void modifyTable(String tableName) throws Exception {
        admin.modifyTable(admin.getDescriptor(TableName.valueOf(tableName)));
    }

    @Override
    public void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception {
        for (String col : addColumn) {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build();
            admin.addColumnFamily(TableName.valueOf(tableName), cf);
        }

        for (String col : removeColumn) {
            admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(col));
        }
    }

    @Override
    public void modifyTable(String tableName, ColumnFamilyDescriptor cfds) throws Exception {
        admin.modifyColumnFamily(TableName.valueOf(tableName), cfds);
    }

    @Override
    public void addData(String tableName, String rowKey, String[] column, String[] value) throws Exception {
        // 设置rowkey
        Put put = new Put(Bytes.toBytes(rowKey));

        Table table = connection.getTable(TableName.valueOf(tableName));
        // TODO 根据 rowkey 增加数据？


    }

    @Override
    public void putData(String tableName, String rowKey, String familyName, String columnName, String value) throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        Table table = connection.getTable(TableName.valueOf(tableName));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
    }

    @Override
    public void putData(String tableName, String rowKey, String familyName, String columnName, String value, long timestamp) throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        Table table = connection.getTable(TableName.valueOf(tableName));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), timestamp, Bytes.toBytes(value));
        table.put(put);
    }

    @Override
    public void putData(Put put) throws Exception {
        //TODO ?? 直接put给谁
    }

    @Override
    public void putData(List<Put> putList) throws Exception {

    }

    @Override
    public Result getResult(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.get(get);
    }

    @Override
    public Result getResult(String tableName, String rowKey, String familyName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(familyName));
        return table.get(get);
    }

    @Override
    public Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        return table.get(get);
    }

    @Override
    public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName, int versions) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.readVersions(versions);
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        return table.get(get);
    }

    @Override
    public ResultScanner getResultScann(String tableName) throws Exception {
        Scan scan = new Scan();
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table.getScanner(scan);
    }

    @Override
    public ResultScanner getResultScann(String tableName, Scan scan) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table.getScanner(scan);
    }

    @Override
    public ResultScanner getResultScann(String tableName, String columnFamily, String qualifier) throws Exception {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table.getScanner(scan);
    }

    @Override
    public void deleteColumn(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteColumn);
    }

    @Override
    public void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addFamily(Bytes.toBytes(falilyName));
        table.delete(deleteColumn);
    }

    @Override
    public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
    }
}
