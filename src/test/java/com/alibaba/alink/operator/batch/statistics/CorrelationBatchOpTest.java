package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class CorrelationBatchOpTest {


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

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        CorrelationBatchOp corr = new CorrelationBatchOp()
            .setSelectedCols(new String[]{"f_double", "f_int", "f_long"})
            .setMethod("pearson");

        corr.linkFrom(source);

        CorrelationResult corrMat = corr.collectCorrelationResult();

        System.out.println(corrMat);


        Assert.assertArrayEquals(corrMat.getCorrelationMatrix().getArrayCopy1D(true),
            new double[]{1.0, -1.0, -1.0,
                -1.0, 1.0, 1.0,
                -1.0, 1.0, 1.0},
            10e-4);
    }
}