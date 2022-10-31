package com.alibaba.alink.pipeline;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.feature.Binarizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizer;
import com.alibaba.alink.pipeline.feature.QuantileDiscretizerModel;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class PipelineTest extends AlinkTestBase {

	/**
	 * Create a temp transformer for test.
	 *
	 * @param name name of the transformer
	 * @return the transformer
	 */
	private TransformerBase mockTransformer(String name) {
		return new TempTransformer(TempMapper::new, new Params().set("name", name));
	}

	@Test
	public void test() throws Exception {
		CsvSourceBatchOp source = new CsvSourceBatchOp()
			.setSchemaStr(
				"sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv");

		String pipeline_model_filename = "/tmp/model123123123123.csv";
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

	/**
	 * Create an estimator and model pair for test.
	 *
	 * @param name name postfix of estimator.
	 * @return estimator and model pair.
	 */
	private Pair <EstimatorBase, ModelBase> mockEstimator(String name) {
		return ImmutablePair.of(new TempEstimatorBase(name), new TempMapModel(name));
	}

	@Test
	public void testFit() throws Exception {
		BatchOperator data = new MemSourceBatchOp(new Object[] {"init"}, "name");

		TransformerBase stage1 = mockTransformer("stage1");
		TransformerBase stage2 = mockTransformer("stage2");
		Pair <EstimatorBase, ModelBase> stage3 = mockEstimator("stage3");
		TransformerBase stage4 = mockTransformer("stage4");
		Pair <EstimatorBase, ModelBase> stage5 = mockEstimator("stage5");
		TransformerBase stage6 = mockTransformer("stage6");

		Pipeline pipe = new Pipeline().add(stage1).add(stage2).add(stage3.getLeft())
			.add(stage4).add(stage5.getLeft()).add(stage6);
		List <Row> results = pipe.fitAndTransform(data).collect();

		assertEquals("init_stage1_stage2_stage3_stage4_stage5_stage6", results.get(0).getField(0));

		results = stage1.transform(data).collect();
		assertEquals("init_stage1", results.get(0).getField(0));
		results = stage2.transform(data).collect();
		assertEquals("init_stage2", results.get(0).getField(0));
		results = stage3.getValue().transform(data).collect();
		assertEquals("init_stage3", results.get(0).getField(0));
		results = stage4.transform(data).collect();
		assertEquals("init_stage4", results.get(0).getField(0));
		results = stage5.getValue().transform(data).collect();
		assertEquals("init_stage5", results.get(0).getField(0));
		results = stage5.getValue().transform(data).collect();
		assertEquals("init_stage5", results.get(0).getField(0));
	}

	@Test
	public void testFitWithoutEstimators() throws Exception {
		BatchOperator data = new MemSourceBatchOp(new Object[] {"init"}, "name");

		TransformerBase stage1 = mockTransformer("stage1");
		TransformerBase stage2 = mockTransformer("stage2");
		TransformerBase stage3 = mockTransformer("stage3");
		TransformerBase stage4 = mockTransformer("stage4");

		Pipeline pipe = new Pipeline().add(stage1).add(stage2).add(stage3).add(stage4);
		List <Row> results = pipe.fitAndTransform(data).collect();

		assertEquals("init_stage1_stage2_stage3_stage4", results.get(0).getField(0));

		results = stage1.transform(data).collect();
		assertEquals("init_stage1", results.get(0).getField(0));
		results = stage2.transform(data).collect();
		assertEquals("init_stage2", results.get(0).getField(0));
		results = stage3.transform(data).collect();
		assertEquals("init_stage3", results.get(0).getField(0));
		results = stage4.transform(data).collect();
		assertEquals("init_stage4", results.get(0).getField(0));
	}

	public static class TempTransformer extends MapTransformer {

		protected TempTransformer(
			BiFunction <TableSchema, Params, Mapper> mapperBuilder,
			Params params) {
			super(mapperBuilder, params);
		}
	}

	public static class TempMapper extends Mapper {

		public TempMapper(TableSchema dataSchema, Params params) {
			super(dataSchema, params);
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			result.set(0, selection.get(0) + "_" + params.get("name", String.class));
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema dataSchema,
																							   Params params) {
			return Tuple4.of(new String[] {"name"}, new String[] {"name"}, new TypeInformation[] {Types.STRING()},
				new String[] {});
		}
	}

	public static class TempMapModel extends MapModel {

		public TempMapModel(String name) {
			super(TempModelMapper::new, new Params().set("name", name));
			modelData = new MemSourceBatchOp(new Object[] {"init"}, "model");
		}
	}

	public static class TempEstimatorBase extends EstimatorBase {

		public TempEstimatorBase(String name) {
			super(new Params().set("name", name));
		}

		@Override
		public ModelBase fit(BatchOperator input) {
			return new TempMapModel(params.get("name", String.class));
		}
	}

	public static class TempModelMapper extends ModelMapper {

		public TempModelMapper(Object o, Object o1, Object o2) {
			super((TableSchema) o, (TableSchema) o1, (Params) o2);
		}

		@Override
		protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
			result.set(0, selection.get(0) + "_" + params.get("name", String.class));
		}

		@Override
		protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(TableSchema modelSchema,
																							   TableSchema dataSchema,
																							   Params params) {
			return Tuple4.of(new String[] {"name"}, new String[] {"name"}, new TypeInformation[] {Types.STRING()},
				new String[] {});
		}

		@Override
		public void loadModel(List <Row> modelRows) {

		}
	}
}