package org.jikeshij.zly.main;

import org.apache.hadoop.hbase.client.Put;
import org.jikeshij.zly.Imp.HBaseImp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HBaseMain {
    private static final Logger logger = LoggerFactory.getLogger(HBaseMain.class);
    public static void main(String[] args) {
        String tableName="zhangluyuan:zhangluyuan";
        String[] rowkeys={"1","2","3","4","5"};
        String column_name1="student_id";
        String column_name2="class";
        String column_name3="understanding";
        String column_name4="programming";
        String column_name5="name";
        HBaseImp hBaseImp=new HBaseImp();
        String[] columnFamilys={"name","info","score"};
        hBaseImp.createTable(tableName,columnFamilys);
       logger.info("写入数据");
        Put put=null;
       for(String rowkey:rowkeys){
           put=new Put(rowkey.getBytes());
       }
           put.addColumn(columnFamilys[0].getBytes(), column_name5.getBytes(), "Jom".getBytes());
           put.addColumn(columnFamilys[0].getBytes(), column_name5.getBytes(), "Jerry".getBytes());
           put.addColumn(columnFamilys[0].getBytes(), column_name5.getBytes(), "Jack".getBytes());
           put.addColumn(columnFamilys[0].getBytes(), column_name5.getBytes(), "Rose".getBytes());
           put.addColumn(columnFamilys[0].getBytes(), column_name5.getBytes(), "zhangluyuan".getBytes());
           put.addColumn(columnFamilys[1].getBytes(), column_name1.getBytes(), "20210000000001".getBytes());
           put.addColumn(columnFamilys[1].getBytes(), column_name1.getBytes(), "20210000000002".getBytes());
           put.addColumn(columnFamilys[1].getBytes(), column_name1.getBytes(), "20210000000003".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name1.getBytes(), "20210000000004".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name1.getBytes(), "G20210735010132".getBytes());

           put.addColumn( columnFamilys[1].getBytes(), column_name2.getBytes(), "1".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name2.getBytes(), "1".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name2.getBytes(), "2".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name2.getBytes(), "2".getBytes());
           put.addColumn( columnFamilys[1].getBytes(), column_name2.getBytes(), "2".getBytes());

           put.addColumn( columnFamilys[2].getBytes(), column_name3.getBytes(), "75".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name3.getBytes(), "85".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name3.getBytes(), "80".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name3.getBytes(), "60".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name3.getBytes(), "70".getBytes());

           put.addColumn( columnFamilys[2].getBytes(), column_name4.getBytes(), "82".getBytes());
           put.addColumn(columnFamilys[2].getBytes(), column_name4.getBytes(), "67".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name4.getBytes(), "80".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name4.getBytes(), "61".getBytes());
           put.addColumn( columnFamilys[2].getBytes(), column_name4.getBytes(), "80".getBytes());
        List<Put> list = new ArrayList<Put>();
        list.add(put);
        hBaseImp.putRows(tableName,list);
           logger.info("读取数据");
           hBaseImp.getRow(tableName,"1");
           logger.info("扫描全表");
           hBaseImp.getScanner(tableName);
           logger.info("删除数据");
//           hBaseImp.deleteQualifier(tableName,"1",columnFamilys[0],column_name1);
           logger.info("删除表");
//           hBaseImp.deleteTable(tableName);
    }
}
