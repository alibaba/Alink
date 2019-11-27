package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class ChiSqSelectorBatchOpTest {

    @Test
    public void test() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1L, 1, 2.0, true),
                Row.of(null, 2L, 2, -3.0, true),
                Row.of("c", null, null, 2.0, false),
                Row.of("a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"};

        MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        ChiSqSelectorBatchOp selector = new ChiSqSelectorBatchOp()
            .setSelectedCols(new String[]{"f_string", "f_long", "f_int", "f_double"})
            .setLabelCol("f_boolean")
            .setNumTopFeatures(2);

        selector.linkFrom(data);

        String[] selectedColNames = selector.collectResult();

        assertArrayEquals(new String[]{"f_string", "f_long"}, selectedColNames);
    }

}