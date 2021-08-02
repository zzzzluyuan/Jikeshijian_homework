package org.jikeshij.zly.rpc.impl;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfaceImpl implements MyInterface {
    //实现加法
    @Override
    public int add(int number1, int number2){
        System.out.println("number1 = " + number1 + "number2 = " + number2);
        return number1 + number2;
    }
    //返回版本号
    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException{
        return MyInterface.versionID;
    }

    @Override
    public String getName(String stunum) {
        if (stunum.equals("G20210735010132")){
            System.out.println("Server: 此学生的学号是" + stunum );
            return "张露元";
        }else if (stunum.equals("12312312")){
            System.out.println("Server: 此学生的学号是" + stunum );
            return "张三";
        }else {
            System.out.println("Server: 抱歉，无法找到学号" + stunum );
            return "null";
        }
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }


}