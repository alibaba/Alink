package com.alibaba.alink.pipeline;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.pipeline.clustering.Lda;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_DATA_ENABLED;
import static com.alibaba.alink.common.lazy.HasLazyPrintTransformInfo.LAZY_PRINT_TRANSFORM_STAT_ENABLED;

public class PipelineModelTest {

	@Test
	public void test() throws Exception {
		BatchOperator source = getTrainSource();
		StreamOperator streamSource= getTrainStreamSource();

		Pipeline pipeline = getPipeline();
		PipelineModel pipelineModel = pipeline.fit(source);
		pipelineModel.transform(source).print();

		pipelineModel.transform(streamSource).print();
		StreamOperator.execute();
	}

	protected Pipeline getPipeline() {
		//model mapper
		QuantileDiscretizer quantileDiscretizer = new QuantileDiscretizer()
			.setNumBuckets(2)
			.setSelectedCols("sepal_length");

		//SISO mapper
		Binarizer binarizer = new Binarizer()
			.setSelectedCol("petal_width")
			.setOutputCol("bina")
			.setReservedCols("sepal_length", "petal_width", "petal_length", "category")
			.setThreshold(1.);

		//MISO Mapper
		VectorAssembler assembler = new VectorAssembler()
			.setSelectedCols("sepal_length", "petal_width")
			.setOutputCol("assem")
			.setReservedCols("sepal_length", "petal_width", "petal_length", "category");

		//Lda
		Lda lda = new Lda()
			.setPredictionCol("lda_pred")
			.setPredictionDetailCol("lda_pred_detail")
			.setSelectedCol("category")
			.setTopicNum(2)
			.setRandomSeed(0);

		return new Pipeline()
			.add(binarizer)
			.add(assembler)
			.add(quantileDiscretizer)
			.add(lda)
			;
	}

	protected BatchOperator getTrainSource() {
		return new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");
	}

	protected StreamOperator getTrainStreamSource() {
		return new CsvSourceStreamOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv");
	}

	@Test
	public void pipelineTestSetLazy() throws Exception {
		String[] binaryNames = new String[] {"docid", "word", "cnt"};
		TableSchema schema = new TableSchema(
			new String[] {"id", "docid", "word", "cnt"},
			new TypeInformation <?>[] {Types.STRING, Types.STRING, Types.STRING, Types.LONG}
		);

		Row[] array = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc0", "地", 5L),
			Row.of("2", "doc0", "人", 1L),
			Row.of("3", "doc1", null, 3L),
			Row.of("4", null, "人", 2L),
			Row.of("5", "doc1", "合", 4L),
			Row.of("6", "doc1", "一", 4L),
			Row.of("7", "doc2", "清", 3L),
			Row.of("8", "doc2", "一", 2L),
			Row.of("9", "doc2", "色", 2L)
		};

		BatchOperator batchSource = new MemSourceBatchOp(Arrays.asList(array), schema);

		OneHotEncoder oneHot = new OneHotEncoder()
			.setSelectedCols(binaryNames)
			.setOutputCols("results")
			.setDropLast(false);

		VectorAssembler va = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.enableLazyPrintTransformData(10, "xxxxxx")
			.setOutputCol("outN");

		VectorAssembler va2 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.setOutputCol("outN");

		VectorAssembler va3 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.setOutputCol("outN");

		VectorAssembler va4 = new VectorAssembler()
			.setSelectedCols(new String[] {"cnt", "results"})
			.enableLazyPrintTransformStat("xxxxxx4")
			.setOutputCol("outN");

		Pipeline pl = new Pipeline().add(oneHot).add(va).add(va2).add(va3).add(va4);

		PipelineModel model = pl.fit(batchSource);

		Row[] parray = new Row[] {
			Row.of("0", "doc0", "天", 4L),
			Row.of("1", "doc2", null, 3L)
		};

		// batch predict
		MemSourceBatchOp predData = new MemSourceBatchOp(Arrays.asList(parray), schema);
		BatchOperator result = model.transform(predData).select(new String[] {"docid", "outN"});

		List <Row> rows = result.collect();

		for (Row row : rows) {
			if (row.getField(0).toString().equals("doc0")) {
				Assert.assertEquals(VectorUtil.getVector(row.getField(1).toString()).size(), 19);
			} else if (row.getField(0).toString().equals("doc2")) {
				Assert.assertEquals(VectorUtil.getVector(row.getField(1).toString()).size(), 19);
			}
		}

		// stream predict
		MemSourceStreamOp predSData = new MemSourceStreamOp(Arrays.asList(parray), schema);
		model.transform(predSData).print();
		StreamOperator.execute();
	}

	@Test
	public void testSplitModel() {
		PipelineModel model = getPipelineModel();
		List <PipelineModel>  splitModel = model.splitPipelineModel(true);
		Assert.assertEquals(splitModel.size(), 1);

		model.transformers[0].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, true);
		model.transformers[3].set(LAZY_PRINT_TRANSFORM_STAT_ENABLED, true);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(3, splitModel2.size());
		Assert.assertEquals(1, splitModel2.get(0).transformers.length);
		Assert.assertEquals(3, splitModel2.get(1).transformers.length);
		Assert.assertEquals(1, splitModel2.get(2).transformers.length);
	}

	@Test
	public void testSplitModel2() {
		PipelineModel model = getPipelineModel();
		model.transformers[0].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, true);
		model.transformers[3].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, true);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(3, splitModel2.size());
		Assert.assertEquals(1, splitModel2.get(0).transformers.length);
		Assert.assertEquals(3, splitModel2.get(1).transformers.length);
		Assert.assertEquals(1, splitModel2.get(2).transformers.length);
	}

	@Test
	public void testSplitModel3() {
		PipelineModel model = getPipelineModel();
		model.transformers[0].set(MapperParams.NUM_THREADS, 2);
		model.transformers[3].set(MapperParams.NUM_THREADS, 2);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(1, splitModel2.size());
		Assert.assertEquals(5, splitModel2.get(0).transformers.length);
	}

	@Test
	public void testSplitModel4() {
		PipelineModel model = getPipelineModel();
		model.transformers[0].set(MapperParams.NUM_THREADS, 2);
		model.transformers[3].set(MapperParams.NUM_THREADS, 3);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(2, splitModel2.size());
		Assert.assertEquals(3, splitModel2.get(0).transformers.length);
		Assert.assertEquals(2, splitModel2.get(1).transformers.length);
	}

	@Test
	public void testSplitModel5() {
		PipelineModel model = getPipelineModel();
		model.transformers[0].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, true);
		model.transformers[3].set(MapperParams.NUM_THREADS, 3);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(2, splitModel2.size());
		Assert.assertEquals(1, splitModel2.get(0).transformers.length);
		Assert.assertEquals(4, splitModel2.get(1).transformers.length);
	}

	@Test
	public void testSplitModel6() {
		PipelineModel model = getPipelineModel();
		model.transformers[0].set(LAZY_PRINT_TRANSFORM_DATA_ENABLED, true);
		model.transformers[4].set(MapperParams.NUM_THREADS, 3);

		List <PipelineModel>  splitModel2 = model.splitPipelineModel(true);
		Assert.assertEquals(2, splitModel2.size());
		Assert.assertEquals(1, splitModel2.get(0).transformers.length);
		Assert.assertEquals(4, splitModel2.get(1).transformers.length);
	}

	private PipelineModel getPipelineModel() {
		Row[] rows = new Row[] {
			Row.of(-1L, "{\"stages\":\"[{\\\"identifier\\\":null,\\\"params\\\":null,\\\"schemaIndices\\\":[1,0,2],"
				+ "\\\"colNames\\\":null,\\\"parent\\\":-1},{\\\"identifier\\\":\\\"com.alibaba.alink.pipeline.feature"
				+ ".OneHotEncoderModel\\\",\\\"params\\\":{\\\"params\\\":{\\\"dropLast\\\":\\\"false\\\","
				+ "\\\"selectedCols\\\":\\\"[\\\\\\\"docid\\\\\\\",\\\\\\\"word\\\\\\\",\\\\\\\"cnt\\\\\\\"]\\\","
				+ "\\\"outputCols\\\":\\\"[\\\\\\\"results\\\\\\\"]\\\"}},\\\"schemaIndices\\\":[1,0,2],"
				+ "\\\"colNames\\\":[\\\"column_index\\\",\\\"token\\\",\\\"token_index\\\"],\\\"parent\\\":0},"
				+ "{\\\"identifier\\\":\\\"com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler\\\","
				+ "\\\"params\\\":{\\\"params\\\":{\\\"outputCol\\\":\\\"\\\\\\\"outN\\\\\\\"\\\","
				+ "\\\"selectedCols\\\":\\\"[\\\\\\\"cnt\\\\\\\",\\\\\\\"results\\\\\\\"]\\\"}},"
				+ "\\\"schemaIndices\\\":[1,0,2],\\\"colNames\\\":null,\\\"parent\\\":0},{\\\"identifier\\\":\\\"com"
				+ ".alibaba.alink.pipeline.dataproc.vector.VectorAssembler\\\","
				+ "\\\"params\\\":{\\\"params\\\":{\\\"outputCol\\\":\\\"\\\\\\\"outN\\\\\\\"\\\","
				+ "\\\"selectedCols\\\":\\\"[\\\\\\\"cnt\\\\\\\",\\\\\\\"results\\\\\\\"]\\\"}},"
				+ "\\\"schemaIndices\\\":[1,0,2],\\\"colNames\\\":null,\\\"parent\\\":0},{\\\"identifier\\\":\\\"com"
				+ ".alibaba.alink.pipeline.dataproc.vector.VectorAssembler\\\","
				+ "\\\"params\\\":{\\\"params\\\":{\\\"outputCol\\\":\\\"\\\\\\\"outN\\\\\\\"\\\","
				+ "\\\"selectedCols\\\":\\\"[\\\\\\\"cnt\\\\\\\",\\\\\\\"results\\\\\\\"]\\\"}},"
				+ "\\\"schemaIndices\\\":[1,0,2],\\\"colNames\\\":null,\\\"parent\\\":0},{\\\"identifier\\\":\\\"com"
				+ ".alibaba.alink.pipeline.dataproc.vector.VectorAssembler\\\","
				+ "\\\"params\\\":{\\\"params\\\":{\\\"outputCol\\\":\\\"\\\\\\\"outN\\\\\\\"\\\","
				+ "\\\"selectedCols\\\":\\\"[\\\\\\\"cnt\\\\\\\",\\\\\\\"results\\\\\\\"]\\\"}},"
				+ "\\\"schemaIndices\\\":[1,0,2],\\\"colNames\\\":null,\\\"parent\\\":0}]\"}", null, null),
			Row.of(1L, "{\"selectedCols\":\"[\\\"docid\\\",\\\"word\\\",\\\"cnt\\\"]\","
				+ "\"selectedColTypes\":\"[\\\"VARCHAR\\\",\\\"VARCHAR\\\",\\\"BIGINT\\\"]\","
				+ "\"enableElse\":\"false\"}", -1L, null),
			Row.of(1L, "doc2", 0L, 0L),
			Row.of(1L, "doc1", 0L, 1L),
			Row.of(1L, "doc0", 0L, 2L),
			Row.of(1L, "地", 1L, 0L),
			Row.of(1L, "一", 1L, 1L),
			Row.of(1L, "色", 1L, 2L),
			Row.of(1L, "清", 1L, 3L),
			Row.of(1L, "合", 1L, 4L),
			Row.of(1L, "天", 1L, 5L),
			Row.of(1L, "人", 1L, 6L),
			Row.of(1L, "1", 2L, 0L),
			Row.of(1L, "2", 2L, 1L),
			Row.of(1L, "3", 2L, 2L),
			Row.of(1L, "4", 2L, 3L),
			Row.of(1L, "5", 2L, 4L)
		};
		List<Row> pipelineModelData = new ArrayList <>();
		Collections.addAll(pipelineModelData, rows);

		TableSchema modelSchema = new TableSchema(
			new String[]{"id", "p0", "p1", "p2"},
			new TypeInformation[]{Types.LONG, Types.STRING, Types.LONG, Types.LONG}
		);


		List <Tuple3<PipelineStageBase <?>, TableSchema, List <Row>>> stageList =
			ModelExporterUtils.loadStagesFromPipelineModel(pipelineModelData, modelSchema);

		TransformerBase[] transformers = new TransformerBase[stageList.size()];
		for(int i=0; i<stageList.size(); i++) {
			transformers[i] = (TransformerBase)stageList.get(i).f0;
		}

		return new PipelineModel(transformers);
	}


	@Test
	public void test1() {

	}
}
