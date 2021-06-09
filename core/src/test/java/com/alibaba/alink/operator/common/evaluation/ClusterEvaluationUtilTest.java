package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for ClusterEvaluationUtil.
 */

public class ClusterEvaluationUtilTest extends AlinkTestBase {

	@Test
	public void getBasicClusterStatisticsTest() {
		Row[] rows = new Row[] {
			Row.of(0),
			Row.of(0),
			Row.of(0),
			Row.of(1),
			Row.of(1)
		};

		Params params = ClusterEvaluationUtil.getBasicClusterStatistics(Arrays.asList(rows));
		Assert.assertEquals(params.get(ClusterMetrics.COUNT).intValue(), 5);
		Assert.assertEquals(params.get(ClusterMetrics.K).intValue(), 2);
		Assert.assertArrayEquals(params.get(ClusterMetrics.CLUSTER_ARRAY), new String[] {"0", "1"});

	}

	@Test
	public void calMeanAndSumEuclideanTest() {
		Tuple2[] rows0 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {0, 0, 0}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.1, 0.1, 0.1}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.2, 0.2, 0.2}), "0")
		};
		Tuple3 <String, DenseVector, DenseVector> t = ClusterEvaluationUtil.calMeanAndSum(Arrays.asList(rows0),
			3, new EuclideanDistance());
		System.out.println(t);
	}

	@Test
	public void calMeanAndSumCosineTest() {
		Tuple2[] rows0 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {9, 9, 9}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.1, 9.1, 9.1}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.2, 9.2, 9.2}), "1")
		};
		Tuple3 <String, DenseVector, DenseVector> t = ClusterEvaluationUtil.calMeanAndSum(Arrays.asList(rows0),
			3, new CosineDistance());
		System.out.println(t);
	}

	@Test
	public void getClusterStatisticsEuclideanTest() {
		Tuple2[] rows0 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {0, 0, 0}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.1, 0.1, 0.1}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.2, 0.2, 0.2}), "0")
		};

		Tuple3 <String, DenseVector, DenseVector> meanAndSum = ClusterEvaluationUtil.calMeanAndSum(Arrays.asList
				(rows0),
			3, new EuclideanDistance());

		ClusterMetricsSummary clusterMetricsSummary = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
			new EuclideanDistance(), Tuple3.of("0", meanAndSum.f1, meanAndSum.f2));

		Assert.assertEquals(clusterMetricsSummary.k, 1);
		//Tuple6<String, Integer, Double, Double, Double, DenseVector> t = clusterMetricsSummary.map.get(0);
		Assert.assertEquals(clusterMetricsSummary.clusterId.get(0), "0");
		Assert.assertEquals(clusterMetricsSummary.clusterCnt.get(0).intValue(), 3);
		Assert.assertEquals(clusterMetricsSummary.compactness.get(0), 0.115, 0.001);
		Assert.assertEquals(clusterMetricsSummary.distanceSquareSum.get(0), 0.06, 0.01);
		Assert.assertEquals(clusterMetricsSummary.vectorNormL2Sum.get(0), 0.15, 0.01);
		Assert.assertEquals(clusterMetricsSummary.meanVector.get(0), new DenseVector(new double[] {0.1, 0.1, 0.1}));
		Assert.assertEquals(clusterMetricsSummary.k, 1);
		Assert.assertEquals(clusterMetricsSummary.total, 3);
	}

	@Test
	public void getClusterStatisticsCosineTest() {
		Tuple2[] rows0 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {9, 9, 9}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.1, 9.1, 9.1}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.2, 9.2, 9.2}), "1")
		};

		Tuple3 <String, DenseVector, DenseVector> meanAndSum = ClusterEvaluationUtil.calMeanAndSum(Arrays.asList
				(rows0),
			3, new CosineDistance());

		ClusterMetricsSummary clusterMetricsSummary = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
			new CosineDistance(), Tuple3.of("1", meanAndSum.f1, meanAndSum.f2));

		Assert.assertEquals(clusterMetricsSummary.k, 1);
		Assert.assertEquals(clusterMetricsSummary.clusterId.get(0), "1");
		Assert.assertEquals(clusterMetricsSummary.clusterCnt.get(0).intValue(), 3);
		Assert.assertEquals(clusterMetricsSummary.compactness.get(0), 0, 0.001);
		Assert.assertEquals(clusterMetricsSummary.distanceSquareSum.get(0), 0, 0.01);
		Assert.assertEquals(clusterMetricsSummary.vectorNormL2Sum.get(0), 3.0, 0.01);
		Assert.assertEquals(clusterMetricsSummary.meanVector.get(0).normL2Square(), 1.0, 0.01);
		Assert.assertEquals(clusterMetricsSummary.k, 1);
		Assert.assertEquals(clusterMetricsSummary.total, 3);
	}

	@Test
	public void calSilhouetteCoefficientTest() {
		Tuple2[] rows0 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {0, 0, 0}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.1, 0.1, 0.1}), "0"),
			Tuple2.of(new DenseVector(new double[] {0.2, 0.2, 0.2}), "0")
		};

		Tuple2[] rows1 = new Tuple2[] {
			Tuple2.of(new DenseVector(new double[] {9, 9, 9}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.1, 9.1, 9.1}), "1"),
			Tuple2.of(new DenseVector(new double[] {9.2, 9.2, 9.2}), "1")
		};

		Tuple3 <String, DenseVector, DenseVector> meanAndSum1 = ClusterEvaluationUtil.calMeanAndSum(
			Arrays.asList(rows0), 3, new EuclideanDistance());
		ClusterMetricsSummary clusterMetricsSummary1 = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
			new EuclideanDistance(), meanAndSum1);

		Tuple3 <String, DenseVector, DenseVector> meanAndSum2 = ClusterEvaluationUtil.calMeanAndSum(
			Arrays.asList(rows1), 3, new EuclideanDistance());
		ClusterMetricsSummary clusterMetricsSummary2 = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows1),
			new EuclideanDistance(), meanAndSum2);

		ClusterMetricsSummary clusterMetricsSummary = clusterMetricsSummary1.merge(clusterMetricsSummary2);

		for (Tuple2 <Vector, String> row : rows1) {
			Assert.assertEquals(0.99, ClusterEvaluationUtil.calSilhouetteCoefficient(row, clusterMetricsSummary).f0,
				0.01);
		}

		meanAndSum2 = ClusterEvaluationUtil.calMeanAndSum(Arrays.asList(rows1), 3, new CosineDistance());
		clusterMetricsSummary1 = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows1),
			new CosineDistance(), meanAndSum2);

		Assert.assertEquals(
			(double) ClusterEvaluationUtil.calSilhouetteCoefficient(rows1[1], clusterMetricsSummary1).f0, 1.0, 0.01);
	}

	@Test
	public void matrixToParamsTest() {
		long[][] matrix = new long[][] {{5, 1, 2}, {1, 4, 0}, {0, 1, 3}};
		LongMatrix longMatrix = new LongMatrix(matrix);
		Map <Object, Integer> label = new HashMap <>();
		Map <Object, Integer> pred = new HashMap <>();
		label.put("a", 0);
		label.put("b", 1);
		label.put("c", 2);
		pred.put(0, 0);
		pred.put(1, 1);
		pred.put(2, 2);
		Params params = ClusterEvaluationUtil.extractParamsFromConfusionMatrix(longMatrix, label, pred);
		Assert.assertEquals(params.get(ClusterMetrics.NMI), 0.364, 0.001);
		Assert.assertEquals(params.get(ClusterMetrics.PURITY), 0.705, 0.001);
		Assert.assertEquals(params.get(ClusterMetrics.RI), 0.68, 0.01);
		Assert.assertEquals(params.get(ClusterMetrics.ARI), 0.24, 0.01);
	}
}