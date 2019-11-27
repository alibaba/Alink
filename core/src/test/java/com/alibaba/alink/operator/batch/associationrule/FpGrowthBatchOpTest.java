package com.alibaba.alink.operator.batch.associationrule;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class FpGrowthBatchOpTest {
    @Test
    public void testFpGrowth() throws Exception {
        Row[] rows = new Row[]{
            Row.of("A,B,C,D"),
            Row.of("B,C,E"),
            Row.of("A,B,C,E"),
            Row.of("B,D,E"),
            Row.of("A,B,C,D"),
        };

        Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[]{"items"});

        FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
            .setItemsCol("items")
            .setMinSupportPercent(0.4)
            .setMinConfidence(0.6);

        fpGrowth.linkFrom(BatchOperator.fromTable(data));
        Assert.assertEquals(fpGrowth.count(), 19);
        Assert.assertEquals(fpGrowth.getSideOutput(0).count(), 27);
    }

}