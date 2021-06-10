package com.alibaba.alink.pipeline;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.sink.CsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PipelineSaveAndLoadTest extends AlinkTestBase {

	BatchOperator <?> data;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setup() {
		data = Iris.getBatchData();
	}

	@Test
	public void testSaveLoadEmpty() {
		Assert.assertEquals(0, Pipeline.collectLoad(new Pipeline().save()).size());
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
		Pipeline pipeline1 = Pipeline.collectLoad(pipeline.save());

		PipelineModel model = PipelineModel.collectLoad(pipeline1.fit(data).save());
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

		Pipeline pipeline = new Pipeline().add(new Pipeline().add(va).add(classifier));
		Pipeline pipeline1 = Pipeline.collectLoad(pipeline.save());
		Assert.assertEquals(PipelineModel.collectLoad(pipeline1.fit(data).save()).transform(Iris.getBatchData())
				.count(),
			150);
	}

	@Test
	public void testNewSave() throws Exception {
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
		PipelineModel model = pipeline.fit(data);

		BatchOperator <?> saved = model.save();

		PipelineModel modelLoaded = PipelineModel.collectLoad(saved);

		Assert.assertEquals(modelLoaded.transform(Iris.getBatchData()).count(), 150);
	}

	@Test
	public void testNewSaveToFile() throws Exception {
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
		PipelineModel model = pipeline.fit(data);

		FilePath filePath = new FilePath(folder.newFile().getAbsolutePath());
		model.save(filePath, true);

		BatchOperator.execute();

		PipelineModel modelLoaded = PipelineModel.load(filePath);

		Assert.assertEquals(modelLoaded.transform(Iris.getBatchData()).count(), 150);
	}

	@Test
	public void testNewSaveToFileMultiFile() throws Exception {
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
		PipelineModel model = pipeline.fit(data);

		FilePath filePath = new FilePath(folder.newFile().getAbsolutePath());
		model.save(filePath, true, 2);

		BatchOperator.execute();

		PipelineModel modelLoaded = PipelineModel.load(filePath);

		Assert.assertEquals(modelLoaded.transform(Iris.getBatchData()).count(), 150);
	}

	@Test
	public void testLocalPredictor() throws Exception {
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
		PipelineModel model = pipeline.fit(data);

		FilePath filePath = new FilePath(folder.newFile().getAbsolutePath());
		model.save(filePath, true);

		BatchOperator.execute();

		LocalPredictor localPredictor = new LocalPredictor(
			filePath,
			new TableSchema(
				ArrayUtils.add(data.getColNames(), "features"),
				ArrayUtils.add(data.getColTypes(), VectorTypes.DENSE_VECTOR)
			)
		);

		Row result = localPredictor.map(
			Row.of(5.1, 3.5, 1.4, 0.2, "Iris-setosanew", new DenseVector(new double[] {5.1, 3.5, 1.4, 0.2})));

		System.out.println(JsonConverter.toJson(result));
	}

	@Test
	public void testLocalPredictorMultiFile() throws Exception {
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
		PipelineModel model = pipeline.fit(data);

		FilePath filePath = new FilePath(folder.newFile().getAbsolutePath());
		model.save(filePath, true, 2);

		BatchOperator.execute();

		LocalPredictor localPredictor = new LocalPredictor(
			filePath,
			new TableSchema(
				ArrayUtils.add(data.getColNames(), "features"),
				ArrayUtils.add(data.getColTypes(), VectorTypes.DENSE_VECTOR)
			)
		);

		Row result = localPredictor.map(
			Row.of(5.1, 3.5, 1.4, 0.2, "Iris-setosanew", new DenseVector(new double[] {5.1, 3.5, 1.4, 0.2})));

		System.out.println(JsonConverter.toJson(result));
	}

	@Test
	public void test() throws Exception {
		String schemaStr
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setSchemaStr(schemaStr)
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");

		String modelFilename = "/tmp/model123";

		QuantileDiscretizer stage1 = new QuantileDiscretizer().setNumBuckets(2).setSelectedCols("sepal_length");
		Binarizer stage2 = new Binarizer().setSelectedCol("petal_width").setThreshold(1.);
		QuantileDiscretizer stage3 = new QuantileDiscretizer().setNumBuckets(4).setSelectedCols("petal_length");
		PipelineModel pipelineModel = new Pipeline(stage1, stage2, stage3).fit(source);
		//System.out.println(pipelineModel.transform(source).getSchema().toString());
		pipelineModel.save(new FilePath(modelFilename), true);
		BatchOperator.execute();

		LocalPredictor predictor = new LocalPredictor(modelFilename, schemaStr);
		Row res = predictor.map(Row.of(1.2, 3.4, 2.4, 3.6, "1"));
		Assert.assertEquals(res.getArity(), 5);
	}

	@Test
	public void test2() throws Exception {
		String model_filename = "/tmp/model2.csv";

		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");

		QuantileDiscretizerTrainBatchOp train = new QuantileDiscretizerTrainBatchOp().setNumBuckets(2).setSelectedCols(
			"petal_length")
			.linkFrom(source);
		train.link(new AkSinkBatchOp().setFilePath(model_filename).setOverwriteSink(true));
		BatchOperator.execute();

		//# save pipeline model data to file
		String pipelineModelFilename = "/tmp/model23424.csv";
		QuantileDiscretizer stage1 = new QuantileDiscretizer().setNumBuckets(2).setSelectedCols("sepal_length");
		Binarizer stage2 = new Binarizer().setSelectedCol("petal_width").setThreshold(1.);
		AkSourceBatchOp modelData = new AkSourceBatchOp().setFilePath(model_filename);
		QuantileDiscretizerModel stage3 = new QuantileDiscretizerModel().setSelectedCols("petal_length").setModelData(
			modelData);
		PipelineModel prevPipelineModel = new Pipeline(stage1, stage2, stage3).fit(source);
		prevPipelineModel.save(pipelineModelFilename, true);
		BatchOperator.execute();
	}

	@Test
	public void test3() throws Exception {
		//# save model data to file (ModelBase)
		String modelFilename = "/tmp/model12341.csv";

		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");

		new QuantileDiscretizer().setNumBuckets(2).setSelectedCols("petal_length")
			.fit(source)
			.getModelData()
			.link(new CsvSinkBatchOp().setFilePath(modelFilename).setOverwriteSink(true));
		BatchOperator.execute();

		//# save pipeline model data to file
		QuantileDiscretizerModel model1 = new QuantileDiscretizer().setNumBuckets(2).setSelectedCols("sepal_length")
			.fit(source);
		Binarizer model2 = new Binarizer().setSelectedCol("petal_width").setThreshold(1.);
		CsvSourceBatchOp modelData = new CsvSourceBatchOp().setFilePath(modelFilename).setSchemaStr(
			"model_id BIGINT, model_info STRING");
		QuantileDiscretizerModel model3 = new QuantileDiscretizerModel().setSelectedCols("petal_length").setModelData(
			modelData);

		CsvSourceStreamOp streamSource = new CsvSourceStreamOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");

		PipelineModel pipelineModel = new PipelineModel(model1, model2, model3);
		pipelineModel = PipelineModel.collectLoad(pipelineModel.save());
		pipelineModel.transform(streamSource).print();
		StreamOperator.execute();
	}

	@Test
	public void testPipelineModelLoadSaveNested() throws Exception {
		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");

		String pipeline_model_filename = "/tmp/model123.csv";
		QuantileDiscretizerModel model1 = new QuantileDiscretizer()
			.setNumBuckets(2)
			.setSelectedCols("sepal_length")
			.fit(source);
		Binarizer model2 = new Binarizer().setSelectedCol("petal_width").setThreshold(1.);

		PipelineModel pipeline_model = new PipelineModel(model1, model2);
		pipeline_model.save(pipeline_model_filename, true);
		BatchOperator.execute();

		pipeline_model = PipelineModel.load(pipeline_model_filename);
		BatchOperator <?> res = pipeline_model.transform(source);
		res.print();
	}
}