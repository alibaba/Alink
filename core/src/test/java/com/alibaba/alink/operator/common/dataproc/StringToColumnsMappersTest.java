package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.CsvToColumnsBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class StringToColumnsMappersTest {
    @Test
    public void testCsvToColumns() throws Exception {
        Row[] rows = new Row[]{
            Row.of("pk1", "a,1.0"),
            Row.of("pk2", "e,2.0"),
            Row.of("pk3", ""),
            Row.of("pk4", "b"),
        };

        BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[]{"id", "content"});

        BatchOperator op = new CsvToColumnsBatchOp()
            .setSelectedCol("content")
            .setHandleInvalidMethod("skip")
            .setSchemaStr("f1 string, f2 double");

        BatchOperator output = data.link(op);
        Assert.assertEquals(output.getColNames().length, 4);
        Assert.assertEquals(output.collect().size(), 4);
    }
}
