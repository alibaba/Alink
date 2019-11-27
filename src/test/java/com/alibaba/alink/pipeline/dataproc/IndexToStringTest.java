package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test cases for {@link IndexToString}.
 */
public class IndexToStringTest {
    private static Row[] rows = new Row[]{
        Row.of("football"),
        Row.of("football"),
        Row.of("football"),
        Row.of("basketball"),
        Row.of("basketball"),
        Row.of("tennis"),
    };

    @Test
    public void testIndexToString() throws Exception {
        BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[]{"f0"});

        StringIndexer stringIndexer = new StringIndexer()
            .setModelName("string_indexer_model")
            .setSelectedCol("f0")
            .setOutputCol("f0_indexed")
            .setStringOrderType("frequency_asc");

        BatchOperator indexed = stringIndexer.fit(data).transform(data);

        IndexToString indexToString = new IndexToString()
            .setModelName("string_indexer_model")
            .setSelectedCol("f0_indexed")
            .setOutputCol("f0_indxed_unindexed");

        List<Row> unindexed = indexToString.transform(indexed).collect();
        unindexed.forEach(row -> {
            Assert.assertEquals(row.getField(0), row.getField(2));
        });
    }
}