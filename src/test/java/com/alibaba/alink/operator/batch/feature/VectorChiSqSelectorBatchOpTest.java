package com.alibaba.alink.operator.batch.feature;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class VectorChiSqSelectorBatchOpTest {

    @Test
    public void testDense() {

        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0 4.0", "a"),
                Row.of("-1.0 -3.0 4.0", "a"),
                Row.of("4.0 2.0 3.0", "b"),
                Row.of("3.4 5.1 5.0", "b")
            };

        String[] colNames = new String[]{"vec", "label"};

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        VectorChiSqSelectorBatchOp selector = new VectorChiSqSelectorBatchOp()
            .setSelectedCol("vec")
            .setLabelCol("label")
            .setNumTopFeatures(2);

        selector.linkFrom(source);

        int[] selectedIndices = selector.collectResult();
        assertArrayEquals(selectedIndices, new int[] {2, 0});
    }
}