package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ChiSquareTestBatchOpTest {

    @Test
    public void test() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1, 1.1, 1.2),
                Row.of("b", -2, 0.9, 1.0),
                Row.of("c", 100, -0.01, 1.0),
                Row.of("d", -99, 100.9, 0.1),
                Row.of("a", 1, 1.1, 1.2),
                Row.of("b", -2, 0.9, 1.0),
                Row.of("c", 100, -0.01, 0.2),
                Row.of("d", -99, 100.9, 0.3)
            };

        String[] colNames = new String[]{"col1", "col2", "col3", "col4"};

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        ChiSquareTestBatchOp test = new ChiSquareTestBatchOp()
            .setSelectedCols("col3", "col1")
            .setLabelCol("col2");

        test.linkFrom(source);

        Assert.assertEquals(test.collectChiSquareTestResult()[0].getP(), 0.004301310843500827, 10e-4);
    }
}