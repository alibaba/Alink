package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.MultiStringIndexerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test cases for {@link MultiStringIndexer}.
 */
public class MultiStringIndexerTest {
    private static Row[] rows = new Row[]{
        Row.of(new Object[]{"a", 1L}),
        Row.of(new Object[]{null, 1L}),
        Row.of(new Object[]{"b", 1L}),
        Row.of(new Object[]{"b", 3L}),

    };

    private Map<String, Long> map1;
    private Map<Long, Long> map2;

    @Before
    public void setup() {
        map1 = new HashMap<>();
        map1.put("a", 1L);
        map1.put("b", 0L);
        map1.put(null, null);

        map2 = new HashMap<>();
        map2.put(1L, 0L);
        map2.put(3L, 1L);
    }

    @Test
    public void testMultiStringIndexer() throws Exception {
        BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[]{"f0", "f1"});

        MultiStringIndexer stringIndexer = new MultiStringIndexer()
            .setSelectedCols("f0", "f1")
            .setOutputCols("f0_index", "f1_index")
            .setHandleInvalid("skip")
            .setStringOrderType("frequency_desc");

        data = stringIndexer.fit(data).transform(data);
        Assert.assertEquals(data.getColNames().length, 4);
        List<Row> result = data.collect();
        Assert.assertEquals(result.size(), 4);

        result.forEach(row -> {
            String token1 = (String) row.getField(0);
            Long token2 = (Long) row.getField(1);
            Assert.assertEquals(map1.get(token1), row.getField(2));
            Assert.assertEquals(map2.get(token2), row.getField(3));
        });
    }

    @Test
    public void testMultiStringIndexerPredictBatchOp() throws Exception {
        BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[]{"f0", "f1"});

        MultiStringIndexerTrainBatchOp stringIndexer = new MultiStringIndexerTrainBatchOp()
            .setSelectedCols("f1", "f0")
            .setStringOrderType("frequency_desc");

        MultiStringIndexerPredictBatchOp predictor = new MultiStringIndexerPredictBatchOp()
            .setSelectedCols("f0")
            .setReservedCols("f0")
            .setOutputCols("f0_index")
            .setHandleInvalid("skip");

        stringIndexer.linkFrom(data);
        data = predictor.linkFrom(stringIndexer, data);

        Assert.assertEquals(data.getColNames().length, 2);
        List<Row> result = data.collect();
        Assert.assertEquals(result.size(), 4);

        result.forEach(row -> {
            String token = (String) row.getField(0);
            Assert.assertEquals(map1.get(token), row.getField(1));
        });
    }
}