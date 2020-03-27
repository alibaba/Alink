package com.alibaba.alink.operator.common.io.pravega;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.util.HashMap;

import static com.alibaba.alink.common.utils.JsonConverter.gson;

public class PravegaJsonRowSerializationSchema implements SerializationSchema<Row> {

    String[] colNames;

    public PravegaJsonRowSerializationSchema(String[] colNames) {
        this.colNames = colNames;
    }

    @Override
    public byte[] serialize(Row row) {
        HashMap<String, Object> map = new HashMap<>();
        for (int i = 0; i < colNames.length; i++) {
            Object obj = row.getField(i);
            if (obj != null) {
                map.put(colNames[i], obj);
            }
        }
        String str = gson.toJson(map);
        try {
            return str.getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }
}