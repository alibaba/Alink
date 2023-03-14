package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.batch.statistics.MdsBatchOp;

import org.junit.Test;

public class MdsBatchOpTest {

    @Test
    public void test() throws Exception {
        RandomTableSourceBatchOp source = new RandomTableSourceBatchOp()
                .setNumRows(100L)
                .setNumCols(5);
        BatchOperator<?> mds = new MdsBatchOp()
                .setReservedCols("col0", "col3")
                .setSelectedCols("col0", "col1", "col2", "col3");
        source.link(mds).print();
    }
}
