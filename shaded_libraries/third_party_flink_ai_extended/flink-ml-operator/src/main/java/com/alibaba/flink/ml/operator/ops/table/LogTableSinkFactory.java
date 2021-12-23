package com.alibaba.flink.ml.operator.ops.table;

import com.alibaba.flink.ml.operator.ops.table.descriptor.LogTable;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.ml.operator.ops.table.descriptor.LogTableValidator.CONNECTOR_RICH_SINK_FUNCTION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

public class LogTableSinkFactory implements TableSinkFactory<Row> {

    @Override
    public TableSink<Row> createTableSink(Context context) {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putProperties(context.getTable().toProperties());

        String serializedRichFunction = null;
        if (properties.containsKey(CONNECTOR_RICH_SINK_FUNCTION)) {
            serializedRichFunction = properties.getString(CONNECTOR_RICH_SINK_FUNCTION);
        }
        if (serializedRichFunction == null) {
            return new LogTableStreamSink(context.getTable().getSchema());
        }

        try {
            RichSinkFunction<Row> richSinkFunction = LogTable.RichSinkFunctionDeserializer.deserialize(serializedRichFunction);
            return new LogTableStreamSink(context.getTable().getSchema(), richSinkFunction);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LogTableStreamSink();
    }

    @Override
    public Map<String, String> requiredContext() {
        return Collections.singletonMap(CONNECTOR_TYPE, "LogTable");
    }

    @Override
    public List<String> supportedProperties() {
        return Collections.singletonList("*");
    }
}
