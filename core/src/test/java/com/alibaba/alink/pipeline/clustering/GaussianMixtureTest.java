package com.alibaba.alink.pipeline.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.clustering.GmmPredictBatchOp;
import com.alibaba.alink.operator.batch.clustering.GmmTrainBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorAssemblerBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.clustering.GmmClusterSummary;
import com.alibaba.alink.operator.common.clustering.GmmModelData;
import com.alibaba.alink.operator.common.clustering.GmmModelDataConverter;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test cases for {@link GaussianMixture}.
 */

public class GaussianMixtureTest extends AlinkTestBase {
	private Row[] denseData1D = new Row[] {
		Row.of("-5.1971"), Row.of("-2.5359"), Row.of("-3.8220"),
		Row.of("-5.2211"), Row.of("-5.0602"), Row.of("4.7118"),
		Row.of("6.8989"), Row.of("3.4592"), Row.of("4.6322"),
		Row.of("5.7048"), Row.of("4.6567"), Row.of("5.5026"),
		Row.of("4.5605"), Row.of("5.2043"), Row.of("6.2734")
	};

	private Row[] denseData2D = new Row[] {
		Row.of("-0.6264538, 0.1836433"), Row.of("-0.8356286 1.5952808"),
		Row.of("0.3295078 -0.8204684"), Row.of("0.4874291 0.7383247"),
		Row.of("0.5757814 -0.3053884"), Row.of("1.5117812 0.3898432"),
		Row.of("-0.6212406 -2.2146999"), Row.of("11.1249309 9.9550664"),
		Row.of("9.9838097 10.9438362"), Row.of("10.8212212 10.5939013"),
		Row.of("10.9189774 10.7821363"), Row.of("10.0745650 8.0106483"),
		Row.of("10.6198257 9.9438713"), Row.of("9.8442045 8.5292476"),
		Row.of("9.5218499 10.4179416")
	};

	private Row[] sparseData2D = new Row[] {
		Row.of("0:-0.6264538 1:0.1836433"), Row.of("0:-0.8356286 1:1.5952808"),
		Row.of("0:0.3295078 1:-0.8204684"), Row.of("0:0.4874291 1:0.7383247"),
		Row.of("0:0.5757814 1:-0.3053884"), Row.of("0:1.5117812 1:0.3898432"),
		Row.of("0:-0.6212406 1:-2.2146999"), Row.of("0:11.1249309 1:9.9550664"),
		Row.of("0:9.9838097 1:10.9438362"), Row.of("0:10.8212212 1:10.5939013"),
		Row.of("0:10.9189774 1:10.7821363"), Row.of("0:10.0745650 1:8.0106483"),
		Row.of("0:10.6198257 1:9.9438713"), Row.of("0:9.8442045 1:8.5292476"),
		Row.of("0:9.5218499 1:10.4179416")
	};

	private final double TOL = 1.0e-2;

	private GmmClusterSummary cluster1D1 = new GmmClusterSummary(0, 2.0 / 3.0,
		new DenseVector(new double[] {5.1604}), new DenseVector(new double[] {0.86644}));

	private GmmClusterSummary cluster1D2 = new GmmClusterSummary(1, 1.0 / 3.0,
		new DenseVector(new double[] {-4.3673}), new DenseVector(new double[] {1.1098}));

	private GmmClusterSummary cluster2D1 = new GmmClusterSummary(0, 0.5333333,
		new DenseVector(new double[] {10.363673, 9.897081}),
		new DenseVector(new double[] {0.2961543, 0.1607830, 1.008878}));

	private GmmClusterSummary cluster2D2 = new GmmClusterSummary(1, 0.4666667,
		new DenseVector(new double[] {0.11731091, -0.06192351}),
		new DenseVector(new double[] {0.62049934, 0.06880802, 1.27431874}));

	private boolean isSameCluster(GmmClusterSummary c1, GmmClusterSummary c2) {
		if (Math.abs(c1.weight - c2.weight) > TOL) {
			return false;
		}
		if (MatVecOp.minus(c1.mean, c2.mean).normInf() > TOL) {
			return false;
		}
		if (MatVecOp.minus(c1.cov, c2.cov).normInf() > TOL) {
			return false;
		}
		return true;
	}

	private void compareClusterSummariesOfTwoClusters(List <GmmClusterSummary> actual,
															 List <GmmClusterSummary> expected) {
		Assert.assertEquals(actual.size(), 2);
		Assert.assertEquals(expected.size(), 2);
		Assert.assertTrue(
			(isSameCluster(actual.get(0), expected.get(0)) && isSameCluster(actual.get(1), expected.get(1))) ||
				(isSameCluster(actual.get(0), expected.get(1)) && isSameCluster(actual.get(1), expected.get(0))));
	}

	// Check whether GMM is converged.
	private boolean converged(GmmModelData modelData) {
		for (int i = 0; i < modelData.k; i++) {
			double norm = modelData.data.get(i).cov.normInf();
			if (norm > 5.0) {
				return false;
			}
		}
		return true;
	}

	@Test
	public void testUnivariate() throws Exception {

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(denseData1D), new String[] {"x"});

		GaussianMixtureModel model = new GaussianMixture()
			.setPredictionCol("cluster_id")
			.setPredictionDetailCol("cluster_detail")
			.setVectorCol("x")
			.setEpsilon(0.)
			.fit(data);

		GmmModelData modelData = new GmmModelDataConverter().load(model.getModelData().collect());
		if (converged(modelData)) {
			List <GmmClusterSummary> actual = new ArrayList <>();
			actual.add(cluster1D1);
			actual.add(cluster1D2);
			compareClusterSummariesOfTwoClusters(actual, modelData.data);
		}
	}

	@Test
	public void testMultivariate() throws Exception {

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(denseData2D), new String[] {"x"});

		GaussianMixtureModel model = new GaussianMixture()
			.setPredictionCol("cluster_id")
			.setPredictionDetailCol("cluster_detail")
			.setVectorCol("x")
			.setEpsilon(0.)
			.fit(data);

		model.transform(data).collect();
	}

	@Test
	public void testLazyPrintClusterSummaries() throws Exception {
		VectorAssemblerBatchOp op = new VectorAssemblerBatchOp()
			.setSelectedCols(Iris.getFeatureColNames())
			.setOutputCol("x")
			.linkFrom(Iris.getBatchData());

		GmmTrainBatchOp gmm = new GmmTrainBatchOp()
			.setVectorCol("x")
			.setK(2)
			.setEpsilon(0.)
			.linkFrom(op);

		GmmPredictBatchOp predict = new GmmPredictBatchOp()
			.setVectorCol("x")
			.setPredictionCol("pred")
			.linkFrom(gmm, op);

		ClusterMetrics eval = new EvalClusterBatchOp()
			.setVectorCol("x")
			.setLabelCol(Iris.getLabelColName())
			.setPredictionCol("pred")
			.linkFrom(predict)
			.collectMetrics();

		Assert.assertEquals(eval.getDb(), 1.15, 0.01);
		Assert.assertEquals(eval.getAri(), 0.35, 0.01);
	}
}