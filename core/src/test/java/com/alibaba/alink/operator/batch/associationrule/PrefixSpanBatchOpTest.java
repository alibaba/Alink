package com.alibaba.alink.operator.batch.associationrule;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

public class PrefixSpanBatchOpTest {
    @Test
    public void testPrefixSpan() throws Exception {
        Row[] rows = new Row[]{
            Row.of("a;a,b,c;a,c;d;c,f"),
            Row.of("a,d;c;b,c;a,e"),
            Row.of("e,f;a,b;d,f;c;b"),
            Row.of("e;g;a,f;c;b;c"),
        };

        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();
        Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[]{"sequence"});

        PrefixSpanBatchOp prefixSpan = new PrefixSpanBatchOp()
            .setItemsCol("sequence")
            .setMinSupportCount(2);

        prefixSpan.linkFrom(BatchOperator.fromTable(data));
        Assert.assertEquals(prefixSpan.count(), 53);
    }

}