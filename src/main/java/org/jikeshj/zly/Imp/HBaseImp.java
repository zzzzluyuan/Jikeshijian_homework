package org.jikeshj.zly.Imp;


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.jikeshj.zly.conn.HBaseConn;
import org.jikeshj.zly.dao.HBaseDao;

import java.io.IOException;
import java.util.List;

public class HBaseImp implements HBaseDao {

    public void createTable(String tableName, String[] columnFamilys) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("The table [" + tableName + "] is already exist.");
            }else{
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                for (String columnFamily : columnFamilys) {
                    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
                }
                admin.createTable(builder.build());
                System.out.println("The table [" + tableName + "] is created success.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("The table [" + tableName + "] does not exist.");
            }else{
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                System.out.println("The table [" + tableName + "] is deleted success.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putRow(String tableName, String rowkey, String columnFamilyName, String qualifer, String data) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Put put=new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(qualifer),Bytes.toBytes(data));
                table.put(put);
                System.out.println("数据插入成功");
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putRows(String tableName, List<Put> puts) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                table.put(puts);
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Result getRow(String tableName, String rowkey) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Get get=new Get(Bytes.toBytes(rowkey));
                return table.get(get);
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Result getRowByRang(String tableName, String rowkey) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Get get=new Get(Bytes.toBytes(rowkey));
                Result result=table.get(get);
                System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
                return result;
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner getScanner(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
               Scan scan=new Scan();
               ResultScanner results=table.getScanner(scan);
                results.forEach(result -> {
                    System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                    System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                    System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                    System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                    System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                    System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
                });
                return results;
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner getScannerByRang(String tableName, String startKey, String stopKey) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Scan scan=new Scan();
                scan.withStartRow(Bytes.toBytes(startKey));
                scan.withStopRow(Bytes.toBytes(stopKey));
                ResultScanner results=table.getScanner(scan);
                results.forEach(result -> {
                    System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                    System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                    System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                    System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                    System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                    System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
                });
                return results;
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ResultScanner getScannerByFilter(String tableName, String startKey, String stopKey) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Scan scan=new Scan();
                scan.withStartRow(Bytes.toBytes(startKey));
                scan.withStopRow(Bytes.toBytes(stopKey));
                ResultScanner results=table.getScanner(scan);
                results.forEach(result -> {
                    System.out.println("rowkey == "+Bytes.toString(result.getRow()));
                    System.out.println("basic:name == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("name"))));
                    System.out.println("basic:age == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("age"))));
                    System.out.println("basic:sex == "+Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("sex"))));
                    System.out.println("basic:salary == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("salary"))));
                    System.out.println("basic:job == "+Bytes.toString(result.getValue(Bytes.toBytes("extend"), Bytes.toBytes("job"))));
                });
                return results;
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void deleteRow(String tableName, String rowkey) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Delete delete=new Delete(Bytes.toBytes(rowkey));
                table.delete(delete);
                System.out.println("Delete Row From table [" + tableName + "] is successed.");
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteColumnFamily(String tableName, String columnFamily) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
               admin.deleteColumnFamily(TableName.valueOf(tableName),Bytes.toBytes(columnFamily));
                System.out.println("Delete ColumnFamily From table [" + tableName + "] is successed.");
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteQualifier(String tableName, String rowkey, String columnFamily, String qualiferName) {
        try {
            HBaseAdmin admin = (HBaseAdmin)HBaseConn.getHBaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))){
                Table table=HBaseConn.getTable(tableName);
                Delete delete=new Delete(Bytes.toBytes(rowkey));
                delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualiferName));
                table.delete(delete);
                System.out.println("Delete Qualifier From table [" + tableName + "] is successed.");
            }else {
                System.out.println("The table [" + tableName + "] does not exist.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
