package com.alibaba.alink.operator.common.io.pravega;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;


public class PravegaCsvRowSerializationSchema implements SerializationSchema<Row> {

    private final String fieldDelimiter;

    public PravegaCsvRowSerializationSchema(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    @Override
    public byte[] serialize(Row row) {
        StringBuilder sbd = new StringBuilder();
        int n = row.getArity();
        for (int i = 0; i < n; i++) {
            Object obj = row.getField(i);
            if (obj != null) {
                sbd.append(obj);
            }
            if (i != n - 1) {
                sbd.append(this.fieldDelimiter);
            }
        }
        String str = sbd.toString();

        try {
            return str.getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row", e);
        }
    }
}