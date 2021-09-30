package com.alibaba.flink.ml.operator.table;

import com.alibaba.flink.ml.operator.util.TypeUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class DebugRowSinkFactory implements TableSinkFactory<Row> {

    @Override
    public TableSink<Row> createTableSink(Context context) {
        final TableSchema schema = context.getTable().getSchema();
        return new TableDebugRowSink(TypeUtil.schemaToRowTypeInfo(schema));
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
