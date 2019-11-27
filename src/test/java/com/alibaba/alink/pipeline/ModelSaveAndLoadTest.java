package com.alibaba.alink.pipeline;

import com.alibaba.alink.common.utils.httpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class ModelSaveAndLoadTest {

    final String MODEL_PATH = "/tmp/model2019110983714134012";
    String filePath = MODEL_PATH + "/pipeline_model.csv";
    BatchOperator data;
    PipelineModel pipelineModel;

    @Before
    public void setup() throws Exception {
        data = Iris.getBatchData();

        VectorAssembler va = new VectorAssembler()
            .setSelectedCols(Iris.getFeatureColNames())
            .setOutputCol("features");

        MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
            .setVectorCol("features")
            .setLabelCol(Iris.getLabelColName())
            .setLayers(new int[]{4, 5, 3})
            .setMaxIter(100)
            .setPredictionCol("pred_label")
            .setPredictionDetailCol("pred_detail")
            .setReservedCols(Iris.getLabelColName());

        Pipeline pipeline = new Pipeline().add(va).add(classifier);
        pipelineModel = pipeline.fit(data);

        pipelineModel.save(filePath);
        BatchOperator.execute();
    }

    @After
    public void clear() throws Exception {
        FileUtils.deleteDirectory(new File(MODEL_PATH));
    }

    @Test
    public void testPipelineModelSaveAndLoad() throws Exception {
        PipelineModel model2 = PipelineModel.load(filePath);
        Assert.assertEquals(model2.transform(data).count(), 150);
    }

    @Test
    public void testLocalPredictor() throws Exception {

        PipelineModel model2 = PipelineModel.load(filePath);

        LocalPredictor localPredictor = model2.getLocalPredictor(data.getSchema());
        Row pred = localPredictor.map(Row.of(4.8, 3.4, 1.9, 0.2, "Iris-setosa"));
        Assert.assertEquals(pred.getArity(), 3);
        Assert.assertEquals(pred.getField(0), "Iris-setosa");
        Assert.assertEquals(pred.getField(1), "Iris-setosa");
    }

    @Test
    public void testSaveLoadSave() throws Exception {
        VectorAssembler va = new VectorAssembler()
            .setSelectedCols(Iris.getFeatureColNames())
            .setOutputCol("features");

        MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
            .setVectorCol("features")
            .setLabelCol(Iris.getLabelColName())
            .setLayers(new int[]{4, 5, 3})
            .setMaxIter(100)
            .setPredictionCol("pred_label")
            .setPredictionDetailCol("pred_detail")
            .setReservedCols(Iris.getLabelColName());

        Pipeline pipeline = new Pipeline().add(va).add(classifier);
        PipelineModel.load(pipeline.fit(data).save()).save().print();
    }


    @Test
    public void testNestedSaveAndLoad() throws Exception {
        VectorAssembler va = new VectorAssembler()
            .setSelectedCols(Iris.getFeatureColNames())
            .setOutputCol("features");

        MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
            .setVectorCol("features")
            .setLabelCol(Iris.getLabelColName())
            .setLayers(new int[]{4, 5, 3})
            .setMaxIter(100)
            .setPredictionCol("pred_label")
            .setPredictionDetailCol("pred_detail")
            .setReservedCols(Iris.getLabelColName());

        Pipeline pipeline = new Pipeline().add(va).add(classifier);
        Pipeline pipeline2 = new Pipeline().add(pipeline);
        Assert.assertEquals(PipelineModel.load(pipeline2.fit(data).save()).transform(Iris.getBatchData()).count(), 150);
    }

}