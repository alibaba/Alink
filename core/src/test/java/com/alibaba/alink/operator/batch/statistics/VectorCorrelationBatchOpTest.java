package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class VectorCorrelationBatchOpTest {

    @Test
    public void test() {

        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0"),
                Row.of("-1.0 -3.0"),
                Row.of("4.0 2.0"),
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

        VectorCorrelationBatchOp corr = new VectorCorrelationBatchOp()
            .setSelectedCol("vec")
            .setMethod("pearson");

        corr.linkFrom(source);

        CorrelationResult corrMat = corr.collectCorrelation();

        System.out.println(corrMat);

        Assert.assertArrayEquals(corrMat.getCorrelationMatrix().getArrayCopy1D(true),
            new double[] {1.0, 0.802955068546966,
                0.802955068546966, 1.0},
            10e-4
        );
    }

}