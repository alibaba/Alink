package com.alibaba.flink.ml.operator.table.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Collections;
import java.util.Map;

public class TableDebugRowDescriptor extends ConnectorDescriptor {
    public TableDebugRowDescriptor() {
        super("TableDebugSink", 1, false);
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return Collections.emptyMap();
    }
}
