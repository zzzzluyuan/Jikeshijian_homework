package org.jikeshij.zly.rpc.server;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.jikeshij.zly.rpc.imp.MyInterface;

import java.io.IOException;

public class MyinterfaceImp implements MyInterface {
    @Override
    public int add(int number1, int number2) {
        System.out.println("number1="+number1+",number2="+number2);
        return number1+number2;
    }

    @Override
    public long getProtocolVersion(String protocol, long clinetVersion) throws IOException {
        return MyInterface.VersionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }

}
