package org.jikeshij.zly.rpc.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.jikeshij.zly.rpc.impl.MyInterface;
import org.jikeshij.zly.rpc.impl.MyInterfaceImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

public class ServerMain {
    public static void main(String[] args) {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        //服务器Ip地
        builder.setBindAddress("127.0.0.1");
        //端口号
        builder.setPort(12345);
        builder.setProtocol(MyInterface.class);
        builder.setInstance(new MyInterfaceImpl());

        try {
            RPC.Server server = builder.build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
