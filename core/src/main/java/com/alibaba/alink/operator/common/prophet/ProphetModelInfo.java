package com.alibaba.alink.operator.common.prophet;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.List;

/**
 * the prophet model info
 */
public class ProphetModelInfo implements Serializable {
    String path;
    public ProphetModelInfo(List<Row> list) {
        this.path = path;
    }
}