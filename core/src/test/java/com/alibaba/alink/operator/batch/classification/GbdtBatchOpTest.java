package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.regression.GbdtRegPredictBatchOp;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp;
import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static com.alibaba.alink.operator.common.tree.parallelcart.BaseGbdtTrainBatchOp.USE_ONEHOT;

/**
 * Test cases for gbdt.
 */
public class GbdtBatchOpTest extends AlinkTestBase {

	@Test
	public void linkFrom() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromVector() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorAssemblerBatchOp vectorAssemblerBatchOp = new VectorAssemblerBatchOp()
			.setSelectedCols(colNames[0], colNames[1])
			.setReservedCols(colNames[2])
			.setOutputCol("vector");

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setVectorCol("vector")
			.setLabelCol(colNames[2])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1)
			.setCriteria(GbdtTrainParams.CriteriaType.XGBOOST);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(vectorAssemblerBatchOp.linkFrom(memSourceBatchOp));

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		Assert.assertEquals(
			testArray.length,
			predictBatchOp.linkFrom(model, vectorAssemblerBatchOp.linkFrom(memSourceBatchOp)).collect().size()
		);
	}

	@Test
	public void linkFromVectorEps() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		VectorAssemblerBatchOp vectorAssemblerBatchOp = new VectorAssemblerBatchOp()
			.setSelectedCols(colNames[0], colNames[1])
			.setReservedCols(colNames[2])
			.setOutputCol("vector");

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp
			(new Params().set(BaseGbdtTrainBatchOp.USE_EPSILON_APPRO_QUANTILE, true))
			.setVectorCol("vector")
			.setLabelCol(colNames[2])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1)
			.setCriteria(GbdtTrainParams.CriteriaType.XGBOOST);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(vectorAssemblerBatchOp.linkFrom(memSourceBatchOp));

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		Assert.assertEquals(
			testArray.length,
			predictBatchOp.linkFrom(model, vectorAssemblerBatchOp.linkFrom(memSourceBatchOp)).collect().size()
		);
	}

	@Test
	public void linkFromSparseVector() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("0:1,1:2", 0),
				Row.of("0:1,1:2", 0),
				Row.of("1:3", 1),
				Row.of("1:2", 0),
				Row.of("0:1,1:3", 1),
				Row.of("0:4,1:3", 1),
				Row.of("0:4,1:4", 1),
				Row.of("0:5,1:3", 0),
				Row.of("0:5,1:4", 0),
				Row.of("0:5,1:2", 1)
			};

		String[] colNames = new String[] {"vector", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setVectorCol("vector")
			.setLabelCol(colNames[1])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromOneHot() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("0:1,1:1", 0),
				Row.of("0:1,1:1", 0),
				Row.of("1:1", 1),
				Row.of("0:1", 0),
				Row.of("0:1,1:1", 1),
				Row.of("0:1,1:1", 1),
				Row.of("0:1,1:1", 1),
				Row.of("0:1,1:1", 0),
				Row.of("0:1,1:1", 0),
				Row.of("0:1,1:1", 1)
			};

		String[] colNames = new String[] {"vector", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp(new Params().set(USE_ONEHOT, true))
			.setVectorCol("vector")
			.setLabelCol(colNames[1])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setVectorCol("vector")
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail");

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromWithCategorical() throws Exception {
		Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, "A", 0),
				Row.of(1, 2, "A", 0),
				Row.of(0, 3, "B", 1),
				Row.of(0, 2, "B", 0),
				Row.of(1, 3, "A", 1),
				Row.of(4, 3, "A", 1),
				Row.of(4, 4, "B", 1),
				Row.of(5, 3, "C", 0),
				Row.of(5, 4, "A", 0),
				Row.of(5, 2, "C", 1)
			};

		String[] colNames = new String[] {"col0", "col1", "col2", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames)
			.setMLEnvironmentId(mlEnvId);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1], colNames[2])
			.setCategoricalCols(colNames[2])
			.setLabelCol(colNames[3])
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(10)
			.setMLEnvironmentId(mlEnvId);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail")
			.setMLEnvironmentId(mlEnvId);

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromWithMissingEps() throws Exception {
		Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();

		Row[] testArray =
			new Row[] {
				Row.of(1, 2.0, null, null, 0),
				Row.of(1, 2.0, "A", null, 0),
				Row.of(0, null, "B", null, 1),
				Row.of(0, 2.0, "B", null, 0),
				Row.of(1, 3.0, "A", null, 1),
				Row.of(null, 3.0, "C", null, 0),
				Row.of(4, 3.0, "A", null, 1),
				Row.of(4, 4.0, "B", null, 1),
				Row.of(5, 4.0, "A", null, 0),
				Row.of(5, 2.0, "C", null, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "col2", "col3", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Arrays.asList(testArray),
			new TableSchema(colNames,
				new TypeInformation[] {Types.INT, Types.DOUBLE, Types.STRING, Types.DOUBLE, Types.INT})
		).setMLEnvironmentId(mlEnvId);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp(
			new Params().set(BaseGbdtTrainBatchOp.USE_EPSILON_APPRO_QUANTILE, true))
			.setFeatureCols(colNames[0], colNames[1], colNames[2], colNames[3])
			.setCategoricalCols(colNames[2])
			.setLabelCol(colNames[4])
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(10)
			.setMLEnvironmentId(mlEnvId);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail")
			.setMLEnvironmentId(mlEnvId);

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromWithMissing() throws Exception {
		Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
		Row[] testArray =
			new Row[] {
				Row.of(1, 2.0, null, null, 0),
				Row.of(1, 2.0, "A", null, 0),
				Row.of(0, null, "B", null, 1),
				Row.of(0, 2.0, "B", null, 0),
				Row.of(1, 3.0, "A", null, 1),
				Row.of(4, 3.0, "A", null, 1),
				Row.of(4, 4.0, "B", null, 1),
				Row.of(null, 3.0, "C", null, 0),
				Row.of(5, 4.0, "A", null, 0),
				Row.of(5, 2.0, "C", null, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "col2", "col3", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Arrays.asList(testArray),
			new TableSchema(colNames,
				new TypeInformation[] {Types.INT, Types.DOUBLE, Types.STRING, Types.DOUBLE, Types.INT})
		).setMLEnvironmentId(mlEnvId);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1], colNames[2], colNames[3])
			.setCategoricalCols(colNames[2])
			.setLabelCol(colNames[4])
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(10)
			.setMLEnvironmentId(mlEnvId);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtPredictBatchOp predictBatchOp = new GbdtPredictBatchOp()
			.setPredictionCol("pred")
			.setPredictionDetailCol("detail")
			.setMLEnvironmentId(mlEnvId);

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromReg() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1),
				Row.of(5, 3, 0),
				Row.of(5, 4, 0),
				Row.of(5, 2, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtRegTrainBatchOp gbdtTrainBatchOp = new GbdtRegTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(10);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		GbdtRegPredictBatchOp predictBatchOp = new GbdtRegPredictBatchOp()
			.setPredictionCol("pred");

		Assert.assertEquals(testArray.length, predictBatchOp.linkFrom(model, memSourceBatchOp).collect().size());
	}

	@Test
	public void linkFromSimple() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMinSamplesPerLeaf(1)
			.setNumTrees(2);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		Assert.assertEquals(
			new GbdtPredictBatchOp()
				.setPredictionCol("pred_col")
				.setPredictionDetailCol("pred_detail")
				.linkFrom(
					model,
					memSourceBatchOp
				)
				.link(
					new EvalBinaryClassBatchOp()
						.setLabelCol(colNames[2])
						.setPositiveLabelValueString("1")
						.setPredictionDetailCol("pred_detail")
				)
				.collectMetrics()
				.getAuc(),
			1.0,
			1e-6
		);
	}

	@Test
	public void testImportance() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(1, 2, 0),
				Row.of(1, 2, 0),
				Row.of(0, 3, 1),
				Row.of(0, 2, 0),
				Row.of(1, 3, 1),
				Row.of(4, 3, 1),
				Row.of(4, 4, 1)
			};

		String[] colNames = new String[] {"col0", "col1", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setFeatureCols(colNames[0], colNames[1])
			.setLabelCol(colNames[2])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(0.1)
			.setFeatureSubsamplingRatio(0.5)
			.setSubsamplingRatio(0.9)
			.setNumTrees(100);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		Assert.assertEquals(2, model.getSideOutput(0).collect().size());
	}

	@Test
	public void testImportanceSparse() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of("0:1,1:2", 0),
				Row.of("0:1,1:2", 0),
				Row.of("1:3", 1),
				Row.of("1:2", 0),
				Row.of("0:1,1:3", 1),
				Row.of("0:4,1:3", 1),
				Row.of("0:4,1:4", 1),
				Row.of("0:5,1:3", 0),
				Row.of("0:5,1:4", 0),
				Row.of("0:5,1:2", 1)
			};

		String[] colNames = new String[] {"vector", "label"};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		GbdtTrainBatchOp gbdtTrainBatchOp = new GbdtTrainBatchOp()
			.setVectorCol("vector")
			.setLabelCol(colNames[1])
			.setMaxLeaves(3)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(1.0)
			.setNumTrees(1);

		BatchOperator <?> model = gbdtTrainBatchOp.linkFrom(memSourceBatchOp);

		Assert.assertEquals(2, model.getSideOutput(0).collect().size());
	}
}