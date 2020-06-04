package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Map;

abstract class FormatReader implements Serializable {

    abstract boolean read(Row row, Map<String, String> out);
}
