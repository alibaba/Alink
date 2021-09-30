package com.alibaba.flink.ml.operator.table;

import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class DebugRowSourceFactory implements TableSourceFactory<Row> {
    @Override
    public TableSource<Row> createTableSource(Context context) {
        return new TableDebugRowSource();
    }

    @Override
    public Map<String, String> requiredContext() {
        return Collections.singletonMap(CONNECTOR_TYPE, "TableDebugSink");
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList("*");
    }
}
