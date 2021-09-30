package com.alibaba.flink.ml.operator.ops.table.descriptor;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.Collections;
import java.util.Map;

public class DummyTable extends ConnectorDescriptor {
    public DummyTable() {
        super("DummyTable", 1, false);
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return Collections.emptyMap();
    }
}
