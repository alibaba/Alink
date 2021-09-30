package com.alibaba.flink.ml.operator.ops.table;

import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class TableDummySinkFactory implements TableSinkFactory<Row> {
    @Override
    public TableSink<Row> createTableSink(Context context) {
        return new TableStreamDummySink();
    }

    @Override
    public Map<String, String> requiredContext() {
        return Collections.singletonMap(CONNECTOR_TYPE, "DummyTable");
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList("*");
    }
}
