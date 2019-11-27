package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import org.junit.Assert;
import org.junit.Test;

public class MultilayerPerceptronClassifierTest {

    @Test
    public void testMLPC() throws Exception {
        BatchOperator data = Iris.getBatchData();

        MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
            .setFeatureCols(Iris.getFeatureColNames())
            .setLabelCol(Iris.getLabelColName())
            .setLayers(new int[]{4, 5, 3})
            .setMaxIter(100)
            .setPredictionCol("pred_label")
            .setPredictionDetailCol("pred_detail");

        BatchOperator res = classifier.fit(data).transform(data);

        MultiClassMetrics metrics = new EvalMultiClassBatchOp()
            .setPredictionDetailCol("pred_detail")
            .setLabelCol(Iris.getLabelColName())
            .linkFrom(res)
            .collectMetrics();

        Assert.assertTrue(metrics.getAccuracy() > 0.9);

    }
}