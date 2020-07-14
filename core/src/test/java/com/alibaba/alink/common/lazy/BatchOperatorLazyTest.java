package com.alibaba.alink.common.lazy;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.FirstNBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BatchOperatorLazyTest extends BaseLazyTest {
    @Test
    public void testLazyPrintInExecute() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        String title1 = "This is table 1";
        source.lazyPrint(-1, title1);
        MemSourceBatchOp source2 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uu", "ii", "rr"});
        String title2 = "This is table 2";
        source2.lazyPrint(-1, title2);
        BatchOperator.execute();

        Assert.assertTrue(outContent.toString().contains(title1));
        Assert.assertTrue(outContent.toString().contains(title2));
    }

    @Test
    public void testLazyPrintInCollect() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        source.lazyPrint(-1);
        MemSourceBatchOp source2 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uu", "ii", "rr"});
        source2.lazyPrint(-1);

        MemSourceBatchOp source3 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uuu", "iii", "rrr"});
        List<Row> rows = source3.collect();
        System.out.println(rows.size());

        Assert.assertTrue(outContent.toString().contains(String.join("|", source.getColNames())));
        Assert.assertTrue(outContent.toString().contains(String.join("|", source2.getColNames())));
    }

    @Test
    public void testLazyPrintInPrint() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        source.lazyPrint(-1);
        MemSourceBatchOp source2 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uu", "ii", "rr"});
        source2.lazyPrint(-1);

        MemSourceBatchOp source3 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uuu", "iii", "rrr"});
        source3.print();

        Assert.assertTrue(outContent.toString().contains(String.join("|", source.getColNames())));
        Assert.assertTrue(outContent.toString().contains(String.join("|", source2.getColNames())));
        Assert.assertTrue(outContent.toString().contains(String.join("|", source3.getColNames())));
    }

    @Test
    public void testLazyCollectInPrint() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        source.lazyCollect(
                d -> System.out.println("callback: " + d),
                d -> System.out.println("callback2: " + d)
        );

        MemSourceBatchOp source2 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uu", "ii", "rr"});
        source2.lazyPrint(-1);

        MemSourceBatchOp source3 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uuu", "iii", "rrr"});
        source3.print();

        Assert.assertTrue(outContent.toString().contains("callback: "));
        Assert.assertTrue(outContent.toString().contains("callback2: "));
        Assert.assertTrue(outContent.toString().contains(String.join("|", source2.getColNames())));
        Assert.assertTrue(outContent.toString().contains(String.join("|", source3.getColNames())));
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionInLazyCollect() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        source.lazyCollect(
                d -> System.out.println("callback: " + d),
                d -> System.out.println("callback2: " + d),
                d -> Assert.fail()
        );
        BatchOperator.execute();
    }

    @Test
    public void testLazyStatistics() throws Exception {
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        String title = "==== TITLE ====";
        source.lazyPrintStatistics(title);
        BatchOperator.execute();
        Assert.assertTrue(outContent.toString().contains(title));
        Assert.assertTrue(outContent.toString().contains("numMissingValue"));
        Assert.assertTrue(outContent.toString().contains("count"));
        Assert.assertTrue(outContent.toString().contains("normL1"));
        Assert.assertTrue(outContent.toString().contains("normL2"));
    }

    @Test
    public void testMultipleExecute() throws Exception {
        String title = "This is table 1";
        MemSourceBatchOp sourceA
                = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});
        sourceA.lazyPrint(-1, title);
        BatchOperator.execute();

        // should print sourceA
        Assert.assertTrue(outContent.toString().contains(title));
        outContent.reset();

        MemSourceBatchOp sourceB
                = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});
        sourceB.print();

        // should NOT print sourceA
        Assert.assertFalse(outContent.toString().contains(title));
    }

    @Test
    public void testMultipleLinkExecute() throws Exception {
        String title = "This is table 1";

        MemSourceBatchOp sourceA
                = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});
        sourceA.lazyPrint(-1, title);
        FirstNBatchOp firstN = new FirstNBatchOp().setSize(10).linkFrom(sourceA);
        firstN.print();

        // should print sourceA
        Assert.assertTrue(outContent.toString().contains(title));
        outContent.reset();

        FirstNBatchOp firstN2 = new FirstNBatchOp().setSize(10).linkFrom(sourceA);
        firstN2.print();

        // should NOT print sourceA
        Assert.assertFalse(outContent.toString().contains(title));
    }

    @Test
    public void testNonDefaultEnv() throws Exception {
        Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
        MemSourceBatchOp source =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"})
                .setMLEnvironmentId(mlEnvId);
        String title1 = "This is table 1";
        source.lazyPrint(-1, title1);
        MemSourceBatchOp source2 =
                new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"uu", "ii", "rr"})
                .setMLEnvironmentId(mlEnvId);
        String title2 = "This is table 2";
        source2.lazyPrint(-1, title2);

        BatchOperator.execute(MLEnvironmentFactory.get(mlEnvId));
        MLEnvironmentFactory.remove(mlEnvId);

        Assert.assertTrue(outContent.toString().contains(title1));
        Assert.assertTrue(outContent.toString().contains(title2));
    }
}
