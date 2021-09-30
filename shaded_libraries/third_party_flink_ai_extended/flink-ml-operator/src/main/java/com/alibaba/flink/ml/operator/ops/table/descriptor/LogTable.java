package com.alibaba.flink.ml.operator.ops.table.descriptor;


import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.Map;
import java.util.Base64;

import static com.alibaba.flink.ml.operator.ops.table.descriptor.LogTableValidator.CONNECTOR_RICH_SINK_FUNCTION;

public class LogTable extends ConnectorDescriptor {
    DescriptorProperties properties = new DescriptorProperties();
    public LogTable() {
        super("LogTable", 1, false);
    }

    public LogTable richSinkFunction(RichSinkFunction<Row> richSinkFunction) throws IOException {
        properties.putString(CONNECTOR_RICH_SINK_FUNCTION, RichSinkFunctionSerializer.serialize(richSinkFunction));
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }

    public static class RichSinkFunctionSerializer {
        public static String serialize(RichSinkFunction<Row> function) throws IOException {
            SerializedValue<RichSinkFunction<Row>> serializedValue = new SerializedValue<>(function);
            return Base64.getEncoder().encodeToString(serializedValue.getByteArray());
        }

    }

    public static class RichSinkFunctionDeserializer {
        public static RichSinkFunction<Row> deserialize(String base64String) throws IOException, ClassNotFoundException {
            byte[] decode = Base64.getDecoder().decode(base64String);
            SerializedValue<RichSinkFunction<Row>> serializedValue = SerializedValue.fromBytes(decode);
            return serializedValue.deserializeValue(Thread.currentThread().getContextClassLoader());
        }
    }
}
