package com.alibaba.alink.operator.common.prophet;

/**
 * use py4j,the interface that python code implements
 */
public interface ExampleListener {
    public Object notify(Object source);
}
