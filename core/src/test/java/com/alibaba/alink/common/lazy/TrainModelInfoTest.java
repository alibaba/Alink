package com.alibaba.alink.common.lazy;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeModelInfo;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeOperator;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TrainModelInfoTest extends BaseLazyTest {
    @Test
    public void testTrainModelInfo() {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"});
        FakeOperator algo = new FakeOperator()
                .linkFrom(source);

        FakeModelInfo fakeModelSummary = algo.collectModelInfo();
        Assert.assertTrue(fakeModelSummary.toString().matches("FakeModelInfo\\{content='[^']*'}"));

        FakeTrainInfo fakeTrainInfo = algo.collectTrainInfo();
        Assert.assertTrue(fakeTrainInfo.toString().matches("FakeTrainInfo\\{content='[^']*'}"));
    }

    @Test
    public void testTrainModelInfoNonDefaultEnv() {
        Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"})
                .setMLEnvironmentId(mlEnvId);
        FakeOperator algo = new FakeOperator()
                .setMLEnvironmentId(mlEnvId)
                .linkFrom(source);

        FakeModelInfo fakeModelSummary = algo.collectModelInfo();
        Assert.assertTrue(fakeModelSummary.toString().matches("FakeModelInfo\\{content='[^']*'}"));

        FakeTrainInfo fakeTrainInfo = algo.collectTrainInfo();
        Assert.assertTrue(fakeTrainInfo.toString().matches("FakeTrainInfo\\{content='[^']*'}"));

        MLEnvironmentFactory.remove(mlEnvId);
    }

    @Test
    public void testLazyTrainModelInfo() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"})
                .setMLEnvironmentId(mlEnvId);
        FakeOperator algo = new FakeOperator()
                .setMLEnvironmentId(mlEnvId)
                .linkFrom(source);

        algo.lazyCollectModelInfo(d -> {
            System.out.println("======= MODEL INFO =======");
            System.out.println(d);
            Assert.assertTrue(d.toString().matches("FakeModelInfo\\{content='[^']*'}"));
        });

        algo.lazyCollectTrainInfo(d -> {
            System.out.println("======= TRAIN INFO =======");
            System.out.println(d);
            Assert.assertTrue(d.toString().matches("FakeTrainInfo\\{content='[^']*'}"));
        });

        algo.lazyPrintTrainInfo();
        BatchOperator.execute();
    }

    @Test
    public void testLazyTrainModelInfoNonDefaultEnv() throws Exception {
        Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"u", "i", "r"})
                .setMLEnvironmentId(mlEnvId);
        FakeOperator algo = new FakeOperator()
                .setMLEnvironmentId(mlEnvId)
                .linkFrom(source);

        algo.lazyCollectModelInfo(d -> {
            System.out.println("======= MODEL INFO =======");
            System.out.println(d);
            Assert.assertTrue(d.toString().matches("FakeModelInfo\\{content='[^']*'}"));
        });

        algo.lazyCollectTrainInfo(d -> {
            System.out.println("======= TRAIN INFO =======");
            System.out.println(d);
            Assert.assertTrue(d.toString().matches("FakeTrainInfo\\{content='[^']*'}"));
        });

        algo.lazyPrintTrainInfo();
        BatchOperator.execute(MLEnvironmentFactory.get(mlEnvId));
        MLEnvironmentFactory.remove(mlEnvId);
    }
}
