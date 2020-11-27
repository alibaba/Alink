package com.alibaba.alink.pipeline.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MultilayerPerceptronClassifierTest extends AlinkTestBase {

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

    @Test
    public void testDenseMLPC() throws Exception {

        Row[] array = new Row[]{
                Row.of(new Object[]{"$31$01.0 11.0 21.0 301.0", "1.0 1.0 1.0 1.0", 1.0, 1.0, 1.0, 1.0, 1}),
                Row.of(new Object[]{"$31$01.0 11.0 20.0 301.0", "1.0 1.0 0.0 1.0", 1.0, 1.0, 0.0, 1.0, 1}),
                Row.of(new Object[]{"$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1}),
                Row.of(new Object[]{"$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1}),
                Row.of(new Object[]{"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0}),
                Row.of(new Object[]{"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0}),
                Row.of(new Object[]{"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0}),
                Row.of(new Object[]{"$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0})
        };
        String[] veccolNames = new String[]{"svec", "vec", "f0", "f1", "f2", "f3", "label"};

        BatchOperator data = new MemSourceBatchOp(Arrays.asList(array), veccolNames);


        MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
                .setVectorCol("vec")
                .setLabelCol("label")
                .setLayers(new int[]{4, 5, 2})
                .setMaxIter(100)
                .setPredictionCol("pred_label")
                .setPredictionDetailCol("pred_detail");

        BatchOperator res = classifier.fit(data).transform(data);

        MultiClassMetrics metrics = new EvalMultiClassBatchOp()
                .setPredictionDetailCol("pred_detail")
                .setLabelCol("label")
                .linkFrom(res)
                .collectMetrics();

        Assert.assertTrue(metrics.getAccuracy() > 0.9);

    }
}