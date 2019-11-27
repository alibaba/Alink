package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sql.UnionBatchOp;
import org.junit.Assert;
import org.junit.Test;

public class SplitBatchOpTest {
    @Test
    public void split() throws Exception {
        BatchOperator data = Iris.getBatchData();
        data = new AppendIdBatchOp().linkFrom(data);
        BatchOperator spliter = new SplitBatchOp().setFraction(0.1);
        BatchOperator left = spliter.linkFrom(data);
        BatchOperator right = spliter.getSideOutput(0);
        Assert.assertEquals(left.count(), 15);
        Assert.assertEquals(right.count(), 150 - 15);
        Assert.assertEquals(new UnionBatchOp().linkFrom(left, right).count(), 150);
    }
}