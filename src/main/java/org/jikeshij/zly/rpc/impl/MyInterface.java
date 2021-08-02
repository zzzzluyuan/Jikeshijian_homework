package org.jikeshij.zly.rpc.impl;

import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface MyInterface extends VersionedProtocol {
    long versionID = 1L;
    //实现加法
    int add(int number1, int number2);

    //返回版本号
    long getProtocolVersion(String protocol, long clientVersion) throws IOException;

    //返回学号
    String getName(String stunum);
}
