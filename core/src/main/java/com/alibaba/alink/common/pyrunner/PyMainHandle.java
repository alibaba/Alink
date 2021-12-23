package com.alibaba.alink.common.pyrunner;

public interface PyMainHandle extends PyObjHandle {

    int open(String name);

    int close(String name);

    boolean shutdown(String name);

    boolean check();

    <T extends PyObjHandle> T newobj(String pythonClassName);
}
