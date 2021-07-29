package org.jikeshj.zly.dao;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;

public interface HBaseDao {
    public void createTable(String tableName, String[] columnFamilys);
    public  void deleteTable(String tableName);
    public void putRow(String tableName,String rowkey,String columnFamilyName,String qualifer,String data);
    //批量插入
    public void putRows(String tableName, List<Put> puts);
    //查询单条
    public Result getRow(String tableName, String rowkey);
    //根据条件进行查询
    public  Result getRowByRang(String tableName, String rowkey);
    //scan
    public ResultScanner getScanner(String tableName);
    //根据开始和结尾的限制，进行检索
    public ResultScanner getScannerByRang(String tableName,String startKey,String stopKey);
    //根据过滤条件检索
    public ResultScanner getScannerByFilter(String tableName,String startKey,String stopKey);
    //删除表
    public void deleteRow(String tableName,String rowkey);
    //删除列簇
   public void deleteColumnFamily(String tableName,String columnFamily);
   //删除列
   public  void deleteQualifier(String tableName,String rowkey,String columnFamily,String qualiferName);
}
