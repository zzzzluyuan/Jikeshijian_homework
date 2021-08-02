package org.jikeshij.zly.rpc.imp;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol {
    long VersionID=1L;
    int add(int number1,int number2);

}
