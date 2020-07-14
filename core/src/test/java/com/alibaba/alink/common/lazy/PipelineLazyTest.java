package com.alibaba.alink.common.lazy;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeModel;
import com.alibaba.alink.common.lazy.fake_lazy_operators.FakeTrainer;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class PipelineLazyTest extends BaseLazyTest {
    @Test
    public void testTrainer() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});

        FakeTrainer trainer = new FakeTrainer();
        trainer.enableLazyPrintModelInfo();
        trainer.enableLazyPrintModelInfo("===== MODEL INFO =====");
        trainer.enableLazyPrintTrainInfo();
        trainer.enableLazyPrintTrainInfo("===== TRAIN INFO =====");

        trainer.enableLazyPrintTransformStat();
        trainer.enableLazyPrintTransformStat("===== TRAINER TRANSFORM STAT =====");
        trainer.enableLazyPrintTransformData(5);
        trainer.enableLazyPrintTransformData(5, "===== TRAINER TRANSFORM DATA =====");

        FakeModel model = trainer.fit(source);

        BatchOperator output = model.transform(source);
        output.firstN(5).print();

        String content = outContent.toString();
        Assert.assertTrue(content.contains("===== MODEL INFO ====="));
        Assert.assertTrue(content.contains("FakeModelInfo{content='"));
        Assert.assertTrue(content.contains("===== TRAIN INFO ====="));
        Assert.assertTrue(content.contains("FakeTrainInfo{content='"));
        Assert.assertTrue(content.contains("===== TRAINER TRANSFORM DATA ====="));
        Assert.assertTrue(content.contains(String.join("|", source.getColNames())));
        Assert.assertTrue(content.contains("===== TRAINER TRANSFORM STAT ====="));
        Assert.assertTrue(content.contains("count"));
        Assert.assertTrue(content.contains("numMissingValue"));
        Assert.assertTrue(content.contains("normL1"));
        Assert.assertTrue(content.contains("normL2"));
    }

    @Test
    public void testTransformer() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});

        FakeTrainer trainer = new FakeTrainer();
        FakeModel model = trainer.fit(source);
        model.enableLazyPrintTransformData(5, "===== TRANSFORM DATA =====");
        model.enableLazyPrintTransformStat("===== TRANSFORM STAT =====");

        BatchOperator output = model.transform(source);
        output.firstN(5).print();

        String content = outContent.toString();
        Assert.assertTrue(content.contains("===== TRANSFORM DATA ====="));
        Assert.assertTrue(content.contains(String.join("|", source.getColNames())));
        Assert.assertTrue(content.contains("===== TRANSFORM STAT ====="));
        Assert.assertTrue(content.contains("count"));
        Assert.assertTrue(content.contains("numMissingValue"));
        Assert.assertTrue(content.contains("normL1"));
        Assert.assertTrue(content.contains("normL2"));
    }

    @Test
    public void testTrainerNoFit() throws Exception {
        String title = "===== LAZY PRINT CALLBACK ====";

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "i", "r"});
        FakeTrainer fakeTrainer = new FakeTrainer();
        fakeTrainer.enableLazyPrintTrainInfo(title);
        source.print();

        Assert.assertFalse(outContent.toString().contains(title));

        FakeModel fakeModel = fakeTrainer.fit(source);
        BatchOperator output = fakeModel.transform(source);
        output.print();

        // should print fit summary
        Assert.assertTrue(outContent.toString().contains(title));
        Assert.assertTrue(outContent.toString().contains("FakeTrainInfo{content="));
    }

    @Test
    public void testTrainerMultipleFit() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "i", "r"});
        MemSourceBatchOp source1 = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "ii", "rr"});

        FakeTrainer fakeTrainer = new FakeTrainer();
        FakeModel fakeModel = fakeTrainer.fit(source);

        String title = "===== LAZY PRINT CALLBACK =====";
        fakeModel.enableLazyPrintTransformStat(title);

        BatchOperator output;
        output = fakeModel.transform(source);
        output = fakeModel.transform(source1);
        output.print();

        Assert.assertEquals(2, StringUtils.countMatches(outContent.toString(), title));
    }

    @Test
    public void testTrainerMultipleFitAndTransform() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "i", "r"});
        MemSourceBatchOp source1 = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[] {"label", "u", "ii", "rr"});

        FakeTrainer fakeTrainer = new FakeTrainer();

        String title = "===== LAZY PRINT CALLBACK =====";
        fakeTrainer.enableLazyPrintTransformStat(title);

        FakeModel fakeModel1 = fakeTrainer.fit(source);
        FakeModel fakeModel2 = fakeTrainer.fit(source);

        BatchOperator output1 = fakeModel1.transform(source);
        BatchOperator output2 = fakeModel1.transform(source1);

        BatchOperator output3 = fakeModel2.transform(source);
        BatchOperator output4 = fakeModel2.transform(source1);

        output1.print();

        Assert.assertEquals(4, StringUtils.countMatches(outContent.toString(), title));
    }


    @Test
    public void testTrainerMultipleFitAndTransformOverride() throws Exception {
        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"});
        MemSourceBatchOp source1 = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "ii", "rr"});

        FakeTrainer fakeTrainer = new FakeTrainer();

        String title = "===== LAZY PRINT CALLBACK =====";
        fakeTrainer.enableLazyPrintTransformStat(title);

        FakeModel fakeModel1 = fakeTrainer.fit(source);
        FakeModel fakeModel2 = fakeTrainer.fit(source);

        String title2 = "===== LAZY PRINT CALLBACK2 =====";
        fakeModel1.enableLazyPrintTransformStat(title2);

        BatchOperator output1 = fakeModel1.transform(source);
        BatchOperator output2 = fakeModel1.transform(source1);

        BatchOperator output3 = fakeModel2.transform(source);
        BatchOperator output4 = fakeModel2.transform(source1);

        output1.print();

        Assert.assertEquals(2, StringUtils.countMatches(outContent.toString(), title));
        Assert.assertEquals(2, StringUtils.countMatches(outContent.toString(), title2));
    }

    @Test
    public void testTrainerNonDefaultEnv() throws Exception {
        Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();

        MemSourceBatchOp source = new MemSourceBatchOp(Arrays.asList(TRAIN_ARRAY_DATA), new String[]{"label", "u", "i", "r"})
                .setMLEnvironmentId(mlEnvId);

        FakeTrainer trainer = new FakeTrainer().setMLEnvironmentId(mlEnvId);
        trainer.enableLazyPrintModelInfo();
        trainer.enableLazyPrintModelInfo("===== MODEL INFO =====");
        trainer.enableLazyPrintTrainInfo();
        trainer.enableLazyPrintTrainInfo("===== TRAIN INFO =====");

        trainer.enableLazyPrintTransformStat();
        trainer.enableLazyPrintTransformStat("===== TRAINER TRANSFORM STAT =====");
        trainer.enableLazyPrintTransformData(5);
        trainer.enableLazyPrintTransformData(5, "===== TRAINER TRANSFORM DATA =====");

        FakeModel model = trainer.fit(source);

        BatchOperator output = model.transform(source);
        output.firstN(5).print();

        MLEnvironmentFactory.remove(mlEnvId);

        String content = outContent.toString();
        Assert.assertTrue(content.contains("===== MODEL INFO ====="));
        Assert.assertTrue(content.contains("FakeModelInfo{content='"));
        Assert.assertTrue(content.contains("===== TRAIN INFO ====="));
        Assert.assertTrue(content.contains("FakeTrainInfo{content='"));
        Assert.assertTrue(content.contains("===== TRAINER TRANSFORM DATA ====="));
        Assert.assertTrue(content.contains(String.join("|", source.getColNames())));
        Assert.assertTrue(content.contains("===== TRAINER TRANSFORM STAT ====="));
        Assert.assertTrue(content.contains("count"));
        Assert.assertTrue(content.contains("numMissingValue"));
        Assert.assertTrue(content.contains("normL1"));
        Assert.assertTrue(content.contains("normL2"));
    }
}
