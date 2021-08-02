package org.jikeshij.zly.rpc.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.jikeshij.zly.rpc.imp.MyInterface;
import org.jikeshij.zly.rpc.server.MyinterfaceImp;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ServerMain {
    public static void main(String[] args) {
        RPC.Builder builder=new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(12345);
        builder.setProtocol(MyInterface.class);
        builder.setInstance(new MyinterfaceImp());
        try {
            RPC.Server server = builder.build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 1234), new Configuration());
            int res=proxy.add(1,2);
            System.out.println(res);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
