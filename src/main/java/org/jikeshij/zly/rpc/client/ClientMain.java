package org.jikeshij.zly.rpc.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.jikeshij.zly.rpc.impl.MyInterface;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Scanner;

public class ClientMain {
    public static void main(String[] args) {
        try{
            MyInterface proxy = RPC.getProxy(MyInterface.class,1L,
                    new InetSocketAddress("127.0.0.1",12345),new Configuration());
            int res = proxy.add(1,2);
            System.out.println(res);
            System.out.println("请输入");
            Scanner input=new Scanner(System.in);
            String line = null;
            while (true) {
                System.out.println("请输入字符串 : ");
                String x = input.next();
                System.out.println("您输入的是 : " + x);
                System.out.println(proxy.getName(x));
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
