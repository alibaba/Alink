package com.alibaba.alink.pipeline;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ModelSaveAndLoadTest extends AlinkTestBase {

	BatchOperator data;

	@Before
	public void setup() throws Exception {
		data = Iris.getBatchData();
	}

	@Test
	public void testSaveLoadSave() throws Exception {
		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(Iris.getFeatureColNames())
			.setOutputCol("features");

		MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
			.setVectorCol("features")
			.setLabelCol(Iris.getLabelColName())
			.setLayers(new int[] {4, 5, 3})
			.setMaxIter(30)
			.setPredictionCol("pred_label")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols(Iris.getLabelColName());

		Pipeline pipeline = new Pipeline().add(va).add(classifier);
		PipelineModel model = PipelineModel.collectLoad(pipeline.fit(data).save());
		LocalPredictor localPredictor = model.collectLocalPredictor(data.getSchema());
		Row pred = localPredictor.map(Row.of(4.8, 3.4, 1.9, 0.2, "Iris-setosa"));
		Assert.assertEquals(pred.getArity(), 3);
	}

	@Test
	public void testNestedSaveAndLoad() throws Exception {
		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(Iris.getFeatureColNames())
			.setOutputCol("features");

		MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
			.setVectorCol("features")
			.setLabelCol(Iris.getLabelColName())
			.setLayers(new int[] {4, 5, 3})
			.setMaxIter(2)
			.setPredictionCol("pred_label")
			.setPredictionDetailCol("pred_detail")
			.setReservedCols(Iris.getLabelColName());

		Pipeline pipeline = new Pipeline().add(va).add(classifier);
		Pipeline pipeline2 = new Pipeline().add(pipeline);
		Assert.assertEquals(PipelineModel.collectLoad(pipeline2.fit(data).save()).transform(Iris.getBatchData())
				.count(),
			150);
	}

}