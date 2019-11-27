package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.pipeline.LocalPredictor;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OneVsRestTest {
    private BatchOperator data;

    @Before
    public void setup() throws Exception {
        data = Iris.getBatchData();
    }

    @Test
    public void lr() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();

        LogisticRegression lr = new LogisticRegression()
            .setFeatureCols(Iris.getFeatureColNames())
            .setLabelCol(Iris.getLabelColName())
            .setMaxIter(100);

        OneVsRest oneVsRest = new OneVsRest()
            .setClassifier(lr).setNumClass(3);

        OneVsRestModel model = oneVsRest.fit(data);
        model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
        BatchOperator res = model.transform(data);

        MultiClassMetrics metrics = new EvalMultiClassBatchOp()
            .setPredictionDetailCol("pred_detail")
            .setLabelCol(Iris.getLabelColName())
            .linkFrom(res)
            .collectMetrics();
        Assert.assertTrue(metrics.getAccuracy() > 0.9);
    }

    @Test
    public void pipeline() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();

        VectorAssembler va = new VectorAssembler().setSelectedCols(Iris.getFeatureColNames())
            .setOutputCol("features").setReservedCols(Iris.getLabelColName());

        LogisticRegression lr = new LogisticRegression()
            .setVectorCol("features")
            .setLabelCol(Iris.getLabelColName())
            .setPredictionDetailCol("pred_detail")
            .setMaxIter(100);

        OneVsRest oneVsRest = new OneVsRest()
            .setClassifier(lr).setNumClass(3)
            .setPredictionCol("pred_label");

        Pipeline pipeline = new Pipeline().add(va).add(oneVsRest);
        PipelineModel model = pipeline.fit(data);

        BatchOperator res = model.transform(data);

        MultiClassMetrics metrics = new EvalMultiClassBatchOp()
            .setPredictionDetailCol("pred_detail")
            .setLabelCol(Iris.getLabelColName())
            .linkFrom(res)
            .collectMetrics();
        Assert.assertTrue(metrics.getAccuracy() > 0.9);

        LocalPredictor predictor = model.getLocalPredictor(Iris.getBatchData().getSchema());
        Row row = Row.of(1.0, 1.0, 1.0, 1.0, "Iris-versicolor");
        System.out.println(predictor.map(row));
    }

    @Test
    public void gbdtTriCls() throws Exception {
        MLEnvironmentFactory.getDefault().getExecutionEnvironment().startNewSession();

        GbdtClassifier gbdt = new GbdtClassifier()
            .setFeatureCols(Iris.getFeatureColNames())
            .setLabelCol(Iris.getLabelColName())
            .setCategoricalCols()
            .setMaxBins(128)
            .setMaxDepth(5)
            .setNumTrees(10)
            .setMinSamplesPerLeaf(1)
            .setLearningRate(0.3)
            .setMinInfoGain(0.0)
            .setSubsamplingRatio(1.0)
            .setFeatureSubsamplingRatio(1.0);

        OneVsRest oneVsRest = new OneVsRest().setClassifier(gbdt).setNumClass(3);
        OneVsRestModel model = oneVsRest.fit(data);
        model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
        BatchOperator pred = model.transform(data);

        MultiClassMetrics metrics = new EvalMultiClassBatchOp()
            .setPredictionDetailCol("pred_detail")
            .setLabelCol(Iris.getLabelColName())
            .linkFrom(pred)
            .collectMetrics();
        Assert.assertTrue(metrics.getAccuracy() > 0.9);
    }
}
