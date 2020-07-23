package com.alibaba.alink.pipeline.feature;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.NumSeqSourceBatchOp;
import com.alibaba.alink.operator.common.feature.EqualWidthDiscretizerModelInfoBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.function.Consumer;

/**
 * Pipeline test for EqualWidthDiscretizer.
 */
public class EqualWidthDiscretizerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test() throws Exception {
        try {
            NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 10, "col0");

            Pipeline pipeline = new Pipeline()
                .add(new EqualWidthDiscretizer()
                    .setNumBuckets(2)
                    .enableLazyPrintModelInfo()
                    .setSelectedCols("col0"));

            pipeline.fit(numSeqSourceBatchOp).transform(numSeqSourceBatchOp).collect();
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("Should not throw exception here.");
        }
    }

    @Test
    public void testLazy() throws Exception{
        NumSeqSourceBatchOp numSeqSourceBatchOp = new NumSeqSourceBatchOp(0, 1000, "col0");

        EqualWidthDiscretizerTrainBatchOp op = new EqualWidthDiscretizerTrainBatchOp()
            .setSelectedCols("col0")
            .setNumBuckets(2)
            .linkFrom(numSeqSourceBatchOp);

        op.lazyPrintModelInfo();

        op.lazyCollectModelInfo(new Consumer<EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo>() {
            @Override
            public void accept(
                EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo equalWidthDiscretizerModelInfo) {
                Assert.assertEquals(equalWidthDiscretizerModelInfo.getSelectedColsInModel().length, 1);
            }
        });

        BatchOperator.execute();
    }
}