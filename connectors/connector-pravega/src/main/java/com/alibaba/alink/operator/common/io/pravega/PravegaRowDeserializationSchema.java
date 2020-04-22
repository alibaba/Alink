package com.alibaba.alink.operator.common.io.pravega;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;

public class PravegaRowDeserializationSchema<T> implements DeserializationSchema<T> {

    private Class<T> valueType;

    public PravegaRowDeserializationSchema(Class<T> valueType) {
        this.valueType = valueType;
    }

    @Override
    public T deserialize(byte[] event) throws IOException {
        Row row = new Row(1);
        row.setField(0, event != null ? new String(event, Charset.forName("UTF-8")) : null);
        return (T) row;
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(valueType);
    }
}