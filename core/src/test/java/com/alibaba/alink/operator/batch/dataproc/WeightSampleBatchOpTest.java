package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Test for WeightSampleBatchOp.
 */
public class WeightSampleBatchOpTest {
    @Test
    public void testWithoutReplacement() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of("a", 1.3, 1.1),
                Row.of("b", 2.5, 0.9),
                Row.of("c", 100., -0.01),
                Row.of("d", 100., 100.9),
                Row.of("e", 1.4, 1.1),
                Row.of("f", 2.2, 0.9),
                Row.of("g", 100.1, -0.01),
                Row.of("j", 100., 100.9)
            };
        String[] colnames = new String[] {"id", "weight", "col0"};
        MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
        WeightSampleBatchOp sampleBatchOp = new WeightSampleBatchOp()
            .setWeightCol("weight")
            .setRatio(0.3)
            .linkFrom(inOp);

        Assert.assertEquals(sampleBatchOp.count(), 2);
    }

    @Test
    public void testWithReplacement() throws Exception {
        Row[] testArray =
            new Row[] {
                Row.of("a", 1.3, 1.1),
                Row.of("b", 2.5, 0.9),
                Row.of("c", 100., -0.01),
                Row.of("d", 100., 100.9),
                Row.of("e", 1.4, 1.1),
                Row.of("f", 2.2, 0.9),
                Row.of("g", 100.1, -0.01),
                Row.of("j", 100., 100.9)
            };
        String[] colnames = new String[] {"id", "weight", "col0"};
        MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colnames);
        WeightSampleBatchOp sampleBatchOp = new WeightSampleBatchOp()
            .setWeightCol("weight")
            .setRatio(0.4)
            .setWithReplacement(true)
            .linkFrom(inOp);

        Assert.assertEquals(sampleBatchOp.count(), 3);
    }

}