package com.alibaba.alink.common.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.redis.Redis;
import com.alibaba.alink.common.io.redis.RedisClassLoaderFactory;
import com.alibaba.alink.params.io.RedisParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisUtil {
    final RedisClassLoaderFactory factory;
    final Redis redis;
    final TypeSerializer<Row> keySerializer;
    final TypeSerializer<Row> valueSerializer;
    final DataOutputSerializer keyOutputView;
    final DataInputDeserializer keyInputView;
    final DataInputDeserializer valueInputView;
    private static final int START_SIZE_OUTPUT_VIEW = 8 * 1024 * 1024;

    public RedisUtil(Params params, TypeInformation<?>[] keyFieldTypes, TypeInformation<?>[] valueFieldTypes) {
        this.factory = new RedisClassLoaderFactory(params.get(RedisParams.PLUGIN_VERSION));
        redis = RedisClassLoaderFactory.create(factory).create(params);
        RowTypeInfo keyRowTypeInfo = new RowTypeInfo(keyFieldTypes);
        RowTypeInfo valueRowTypeInfo = new RowTypeInfo(valueFieldTypes);
        this.keySerializer = keyRowTypeInfo.createSerializer(new ExecutionConfig());
        this.valueSerializer = valueRowTypeInfo.createSerializer(new ExecutionConfig());
        this.keyOutputView = new DataOutputSerializer(START_SIZE_OUTPUT_VIEW);
        this.keyInputView = new DataInputDeserializer();
        this.valueInputView = new DataInputDeserializer();
    }

    public Map<Row, Row> getRowKeysRowValues() throws IOException {
        List<byte[]> keys = redis.getKeys();
        Map<Row, Row> result = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i);
            byte[] value = redis.get(key);
            keyInputView.setBuffer(key, 0, key.length);
            valueInputView.setBuffer(value, 0, value.length);
            try {
                result.put(keySerializer.deserialize(keyInputView),
                        valueSerializer.deserialize(valueInputView));
            } catch (Exception e) {
            }
        }
        return result;
    }

    public Map<Row, byte[]> getRowKeysByteKeys() throws IOException {
        DataInputDeserializer keyDeserializer = new DataInputDeserializer();
        List<byte[]> byteKeys = redis.getKeys();
        Map<Row, byte[]> result = new HashMap<>();
        for (int i = 0; i < byteKeys.size(); i++) {
            byte[] key = byteKeys.get(i);
            keyDeserializer.setBuffer(key, 0, key.length);
            Row rowKey = null;
            try {
                rowKey = keySerializer.deserialize(keyDeserializer);
            } catch (Exception e) {
                throw new IllegalArgumentException("Row TypeInformation and Redis key are inconsistent");
            }
            result.put(rowKey, key);
        }
        return result;
    }

    public Row getRowValue(Row key) throws IOException {
        byte[] value = redis.get(getByteKey(key));
        if (value == null) {
            return null;
        }
        try {
            valueInputView.setBuffer(value, 0, value.length);
        } catch (Exception e) {
            throw new IllegalArgumentException("Row TypeInformation and Redis Value are inconsistent");
        }
        return valueSerializer.deserialize(valueInputView);
    }

    public boolean containKey(Row key) {
        byte[] value = redis.get(getByteKey(key));
        if (value == null) {
            return false;
        }
        return true;
    }

    public byte[] getByteKey(Row key) {
        try {
            keySerializer.serialize(key, keyOutputView);
        } catch (Exception e) {
            throw new IllegalArgumentException("Row TypeInformation and Redis key are inconsistent");
        }
        int length = keyOutputView.length();
        byte[] ret = Arrays.copyOfRange(keyOutputView.getSharedBuffer(), 0, length);
        keyOutputView.clear();
        return ret;
    }

    protected void close() {
        redis.close();
    }

    @Override
    protected void finalize() {
        this.close();
    }

}
