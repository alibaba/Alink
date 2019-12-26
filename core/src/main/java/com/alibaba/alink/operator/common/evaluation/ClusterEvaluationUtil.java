package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.common.linalg.*;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.buildLabelIndexLabelArray;

/**
 * Cluster evaluation common used functions.
 */
public class ClusterEvaluationUtil implements AllWindowFunction<Row, Row, TimeWindow> {
    public static int COUNT = 0;
    public static int MEAN = 1;
    public static int SUM_2 = 2;

    private ContinuousDistance distance;

    public ClusterEvaluationUtil(ContinuousDistance distance) {
        this.distance = distance;
    }

    public static Params extractParamsFromConfusionMatrix(LongMatrix longMatrix) {
        long[][] matrix = longMatrix.getMatrix();
        long[] actualLabel = longMatrix.getColSums();
        long[] predictLabel = longMatrix.getRowSums();
        long total = longMatrix.getTotal();

        double entropyActual = 0.0;
        double entropyPredict = 0.0;
        double mutualInfor = 0.0;
        double purity = 0.0;
        long tp = 0L;
        long tpFpSum = 0L;
        long tpFnSum = 0L;
        for (long anActualLabel : actualLabel) {
            entropyActual += entropy(anActualLabel, total);
            tpFpSum += combination(anActualLabel);
        }
        entropyActual /= -Math.log(2);
        for (long aPredictLabel : predictLabel) {
            entropyPredict += entropy(aPredictLabel, total);
            tpFnSum += combination(aPredictLabel);
        }
        entropyPredict /= -Math.log(2);
        for (int i = 0; i < matrix.length; i++) {
            long max = 0;
            for (int j = 0; j < matrix[0].length; j++) {
                max = Math.max(max, matrix[i][j]);
                mutualInfor += (0 == matrix[i][j] ? 0.0 :
                    1.0 * matrix[i][j] / total * Math.log(1.0 * total * matrix[i][j] / predictLabel[i] / actualLabel[j]));
                tp += combination(matrix[i][j]);
            }
            purity += max;
        }
        purity /= total;
        mutualInfor /= Math.log(2);
        long fp = tpFpSum - tp;
        long fn = tpFnSum - tp;
        long totalCombination = combination(total);
        long tn = totalCombination - tp - fn - fp;
        double expectedIndex = 1.0 * tpFpSum * tpFnSum / totalCombination;
        double maxIndex = 1.0 * (tpFpSum + tpFnSum) / 2;
        double ri = 1.0 * (tp + tn) / (tp + tn + fp + fn);
        return new Params()
            .set(ClusterMetrics.NMI, 2.0 * mutualInfor / (entropyActual + entropyPredict))
            .set(ClusterMetrics.PURITY, purity)
            .set(ClusterMetrics.RI, ri)
            .set(ClusterMetrics.ARI, (tp - expectedIndex) / (maxIndex - expectedIndex));
    }

    private static long combination(long number){
        return number * (number - 1) / 2;
    }

    private static double entropy(long frequency, long total){
        double ratio = 1.0 * frequency / total;
        return 0 == frequency ? 0.0 : ratio * Math.log(ratio);
    }

    /**
     * Calculate SilhouetteCoefficent.
     *
     * @param row
     * @param clusterMetricsSummary
     * @return
     */
    public static Tuple1<Double> calSilhouetteCoefficient(Row row, ClusterMetricsSummary clusterMetricsSummary) {
        if (!EvaluationUtil.checkRowFieldNotNull(row)) {
            return Tuple1.of(0.);
        }
        String clusterId = row.getField(0).toString();
        Vector vec = VectorUtil.getVector(row.getField(1));
        double currentClusterDissimilarity = 0.0;
        double neighboringClusterDissimilarity = Double.MAX_VALUE;
        if (clusterMetricsSummary.distance instanceof EuclideanDistance) {
            double normSquare = vec.normL2Square();
            for (int i = 0; i < clusterMetricsSummary.k; i++) {
                double dissimilarity = clusterMetricsSummary.clusterCnt.get(i) * normSquare
                    - 2 * clusterMetricsSummary.clusterCnt.get(i) * MatVecOp.dot(vec, clusterMetricsSummary.meanVector.get(i)) + clusterMetricsSummary.vectorNormL2Sum.get(i);
                if (clusterId.equals(clusterMetricsSummary.clusterId.get(i))) {
                    if (clusterMetricsSummary.clusterCnt.get(i) > 1) {
                        currentClusterDissimilarity = dissimilarity / (clusterMetricsSummary.clusterCnt.get(i) - 1);
                    }
                } else {
                    neighboringClusterDissimilarity = Math.min(neighboringClusterDissimilarity,
                        dissimilarity / clusterMetricsSummary.clusterCnt.get(i));
                }
            }
        } else {
            for (int i = 0; i < clusterMetricsSummary.k; i++) {
                double dissimilarity = 1.0 - MatVecOp.dot(vec, clusterMetricsSummary.meanVector.get(i));
                if (clusterId.equals(clusterMetricsSummary.clusterId.get(i))) {
                    if (clusterMetricsSummary.clusterCnt.get(i) > 1) {
                        currentClusterDissimilarity = dissimilarity * clusterMetricsSummary.clusterCnt.get(i) / (clusterMetricsSummary.clusterCnt.get(i) - 1);
                    }
                } else {
                    neighboringClusterDissimilarity = Math.min(neighboringClusterDissimilarity,
                        dissimilarity);
                }
            }
        }
        return Tuple1.of(currentClusterDissimilarity < neighboringClusterDissimilarity ?
            1 - (currentClusterDissimilarity / neighboringClusterDissimilarity) :
            (neighboringClusterDissimilarity / currentClusterDissimilarity) - 1);
    }

    public static Params getBasicClusterStatistics(Iterable<Row> rows){
        Map<String, Double> map = new HashMap<>(0);
        int count = 0;
        for (Row row : rows) {
            if(row != null && row.getField(0) != null) {
                count++;
                String key = row.getField(0).toString();
                map.merge(key, 1.0, (k,v) -> k + 1.0);
            }
        }
        int c = 0;
        double[] values = new double[map.size()];
        String[] keys = new String[map.size()];
        for(Map.Entry<String, Double> entry : map.entrySet()){
            keys[c] = entry.getKey();
            values[c++] = entry.getValue();
        }
        return new Params()
            .set(ClusterMetrics.COUNT, count)
            .set(ClusterMetrics.K, map.size())
            .set(ClusterMetrics.CLUSTER_ARRAY, keys)
            .set(ClusterMetrics.COUNT_ARRAY, values);
    }

    public static ClusterMetricsSummary getClusterStatistics(Iterable<Row> rows, ContinuousDistance distance) {
        List<Vector> list = new ArrayList<>();
        int total = 0;
        String clusterId;
        DenseVector sumVector;

        Iterator<Row> iterator = rows.iterator();
        Row row = null;
        while (iterator.hasNext() && !EvaluationUtil.checkRowFieldNotNull(row)) {
            row = iterator.next();
        }
        if (EvaluationUtil.checkRowFieldNotNull(row)) {
            clusterId = row.getField(0).toString();
            Vector vec = VectorUtil.getVector(row.getField(1));
            Preconditions.checkArgument(vec.size() >= 1, "Vector Size must be at least 1!");
            sumVector = DenseVector.zeros(vec.size());
        } else {
            return null;
        }

        while (null != row) {
            if (EvaluationUtil.checkRowFieldNotNull(row)) {
                Preconditions.checkArgument(row.getField(0).toString().equals(clusterId),
                    "ClusterId must be the same!");
                Vector vec = VectorUtil.getVector(row.getField(1));
                list.add(vec);
                if (distance instanceof EuclideanDistance) {
                    sumVector.plusEqual(vec);
                } else {
                    vec.scaleEqual(1.0 / vec.normL2());
                    sumVector.plusEqual(vec);
                }
                total++;
            }
            row = iterator.hasNext() ? iterator.next() : null;
        }

        DenseVector meanVector = sumVector.scale(1.0 / total);

        double distanceSum = 0.0;
        double distanceSquareSum = 0.0;
        double vectorNormL2Sum = 0.0;
        for (Vector vec : list) {
            double d = distance.calc(meanVector, vec);
            distanceSum += d;
            distanceSquareSum += d * d;
            vectorNormL2Sum += vec.normL2Square();
        }
        return new ClusterMetricsSummary(clusterId, total, distanceSum / total, distanceSquareSum, vectorNormL2Sum,
            meanVector, distance);
    }

    public static class SaveDataAsParams extends RichMapFunction<BaseMetricsSummary, Params> {
        @Override
        public Params map(BaseMetricsSummary t) throws Exception {
            Params params = t.toMetrics().getParams();
            List<Tuple1<Double>> silhouetteCoefficient = getRuntimeContext().getBroadcastVariable(
                EvalClusterBatchOp.SILHOUETTE_COEFFICIENT);
            params.set(ClusterMetrics.SILHOUETTE_COEFFICIENT,
                silhouetteCoefficient.get(0).f0 / params.get(ClusterMetrics.COUNT));
            return params;
        }
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Row> rows, Collector<Row> collector) throws Exception {
        ClusterMetricsSummary metrics = getClusterStatistics(rows, distance);
        collector.collect(Row.of(metrics.toMetrics().serialize()));
    }
}
