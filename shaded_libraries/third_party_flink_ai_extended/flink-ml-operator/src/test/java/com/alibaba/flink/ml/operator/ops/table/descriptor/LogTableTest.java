package com.alibaba.flink.ml.operator.ops.table.descriptor;

import com.alibaba.flink.ml.operator.ops.table.descriptor.LogTable.RichSinkFunctionSerializer;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class LogTableTest {
    RichSinkFunction<Row> function;
    String serializedString;
    @Before
    public void setUp() throws Exception {
        function = new TestRichSinkFunction<>("testpath");
        serializedString = RichSinkFunctionSerializer.serialize(function);
    }

    @Test
    public void testSerializeRichSinkFunction() throws IOException {
        String serialize = RichSinkFunctionSerializer.serialize(function);
        assertNotNull(serialize);
    }

    @Test
    public void testDeserializeRichSinkFunction() throws IOException, ClassNotFoundException {
        RichSinkFunction<Row> func = LogTable.RichSinkFunctionDeserializer.deserialize(serializedString);
        assertNotNull(func);
    }

    @Test
    public void testSerializeAndDeserialize() throws IOException, ClassNotFoundException {
        String base64String = RichSinkFunctionSerializer.serialize(function);
        RichSinkFunction<Row> deserialize = LogTable.RichSinkFunctionDeserializer.deserialize(base64String);
        assertEquals(deserialize, function);
    }

    static class TestRichSinkFunction<T> extends RichSinkFunction<T> {
        private String path;

        TestRichSinkFunction(String path) {
            this.path = path;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (!(obj instanceof TestRichSinkFunction)) {
                return false;
            }

            TestRichSinkFunction<Row> other = (TestRichSinkFunction<Row>) obj;
            return path.equals(other.path);
        }
    }
}