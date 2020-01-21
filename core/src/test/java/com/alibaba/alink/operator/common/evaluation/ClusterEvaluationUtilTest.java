package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit test for ClusterEvaluationUtil.
 */
public class ClusterEvaluationUtilTest {

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
        Assert.assertArrayEquals(params.get(ClusterMetrics.CLUSTER_ARRAY), new String[]{"0", "1"});

    }

    @Test
    public void getClusterStatisticsEuclideanTest() {
        Row[] rows0 = new Row[] {
            Row.of(0, "0,0,0"),
            Row.of(0, "0.1,0.1,0.1"),
            Row.of(0, "0.2,0.2,0.2")
        };

        ClusterMetricsSummary clusterMetricsSummary = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
            new EuclideanDistance());

        Assert.assertEquals(clusterMetricsSummary.k, 1);
        //Tuple6<String, Integer, Double, Double, Double, DenseVector> t = clusterMetricsSummary.map.get(0);
        Assert.assertEquals(clusterMetricsSummary.clusterId.get(0), "0");
        Assert.assertEquals(clusterMetricsSummary.clusterCnt.get(0).intValue(), 3);
        Assert.assertEquals(clusterMetricsSummary.compactness.get(0), 0.115, 0.001);
        Assert.assertEquals(clusterMetricsSummary.distanceSquareSum.get(0), 0.06, 0.01);
        Assert.assertEquals(clusterMetricsSummary.vectorNormL2Sum.get(0), 0.15, 0.01);
        Assert.assertEquals(clusterMetricsSummary.meanVector.get(0), new DenseVector(new double[]{0.1, 0.1, 0.1}));
        Assert.assertEquals(clusterMetricsSummary.k, 1);
        Assert.assertEquals(clusterMetricsSummary.total, 3);
    }

    @Test
    public void getClusterStatisticsCosineTest() {
        Row[] rows0 = new Row[] {
            Row.of(1, "9 9 9"),
            Row.of(1, "9.1 9.1 9.1"),
            Row.of(1, "9.2 9.2 9.2")
        };

        ClusterMetricsSummary clusterMetricsSummary = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
            new CosineDistance());

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
    public void calSilhouetteCoefficientTest(){
        Row[] rows0 = new Row[] {
            Row.of(0, "0,0,0"),
            Row.of(0, "0.1,0.1,0.1"),
            Row.of(0, "0.2,0.2,0.2")
        };

        Row[] rows1 = new Row[] {
            Row.of(1, "9 9 9"),
            Row.of(1, "9.1 9.1 9.1"),
            Row.of(1, "9.2 9.2 9.2")
        };

        ClusterMetricsSummary clusterMetricsSummary1 = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows0),
            new EuclideanDistance());

        ClusterMetricsSummary clusterMetricsSummary2 = ClusterEvaluationUtil.getClusterStatistics(Arrays.asList(rows1),
            new EuclideanDistance());

        ClusterMetricsSummary clusterMetricsSummary = clusterMetricsSummary1.merge(clusterMetricsSummary2);

        for(Row row : rows1){
            Assert.assertEquals(0.99, ClusterEvaluationUtil.calSilhouetteCoefficient(row, clusterMetricsSummary).f0, 0.01);
        }
    }

    @Test
    public void matrixToParamsTest(){
        long[][] matrix = new long[][]{{5, 1, 2}, {1, 4, 0}, {0, 1, 3}};
        LongMatrix longMatrix = new LongMatrix(matrix);

        Params params = ClusterEvaluationUtil.extractParamsFromConfusionMatrix(longMatrix);
        Assert.assertEquals(params.get(ClusterMetrics.NMI), 0.364, 0.001);
        Assert.assertEquals(params.get(ClusterMetrics.PURITY), 0.705, 0.001);
        Assert.assertEquals(params.get(ClusterMetrics.RI), 0.68, 0.01);
        Assert.assertEquals(params.get(ClusterMetrics.ARI), 0.24, 0.01);
    }
}