package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.common.distance.ContinuousDistance;
import com.alibaba.alink.operator.common.distance.CosineDistance;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cluster evaluation common used functions.
 */
public class ClusterEvaluationUtil {
	private static final long serialVersionUID = -7300130718897249710L;
	public static int COUNT = 0;
    public static int MEAN = 1;

    public static Params extractParamsFromConfusionMatrix(LongMatrix longMatrix, Map<Object, Integer> labels, Map<Object, Integer> predictions) {
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

        String[] labelArray = new String[labels.size()];
        String[] predArray = new String[predictions.size()];
        for(Map.Entry<Object, Integer> entry : labels.entrySet()){
            labelArray[entry.getValue()] = entry.getKey().toString();
        }
        for(Map.Entry<Object, Integer> entry : predictions.entrySet()){
            predArray[entry.getValue()] = entry.getKey().toString();
        }
        return new Params()
            .set(ClusterMetrics.NMI, 2.0 * mutualInfor / (entropyActual + entropyPredict))
            .set(ClusterMetrics.PURITY, purity)
            .set(ClusterMetrics.RI, ri)
            .set(ClusterMetrics.ARI, (tp - expectedIndex) / (maxIndex - expectedIndex))
            .set(ClusterMetrics.CONFUSION_MATRIX, matrix)
            .set(ClusterMetrics.LABEL_ARRAY, labelArray)
            .set(ClusterMetrics.PRED_ARRAY, predArray);
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
    public static Tuple1<Double> calSilhouetteCoefficient(Tuple2<Vector, String> row, ClusterMetricsSummary clusterMetricsSummary) {
        String clusterId = row.f1;
        Vector vec = row.f0;
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
            vec.scaleEqual(1.0 / vec.normL2());
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

    public static Tuple3<String, DenseVector, DenseVector> calMeanAndSum(
        Iterable<Tuple2<Vector, String>> rows,
        int vectorSize,
        FastDistance distance){
        int total = 0;
        String clusterId = null;
        DenseVector sumVector = DenseVector.zeros(vectorSize);

        for(Tuple2<Vector, String> t : rows) {
            if(null == clusterId){
                clusterId = t.f1;
            }
            Vector vec = t.f0;
            if (distance instanceof EuclideanDistance) {
                sumVector.plusEqual(vec);
            } else {
                vec.scaleEqual(1.0 / vec.normL2());
                sumVector.plusEqual(vec);
            }
            total++;
        }

        DenseVector meanVector = sumVector.scale(1.0 / total);
        if(distance instanceof CosineDistance){
            meanVector.scaleEqual(1.0 / meanVector.normL2());
        }
        return Tuple3.of(clusterId, meanVector, sumVector);
    }

    public static ClusterMetricsSummary getClusterStatistics(Iterable<Tuple2<Vector, String>> rows,
                                                             ContinuousDistance distance,
                                                             Tuple3<String, DenseVector, DenseVector> meanAndSum) {
        int total = 0;
        String clusterId = meanAndSum.f0;
        DenseVector meanVector = meanAndSum.f1;
        DenseVector sumVector = meanAndSum.f2;

        double distanceSum = 0.0;
        double distanceSquareSum = 0.0;
        double vectorNormL2Sum = 0.0;
        for (Tuple2<Vector, String> t : rows) {
            double d = distance.calc(meanVector, t.f0);
            distanceSum += d;
            distanceSquareSum += d * d;
            vectorNormL2Sum += t.f0.normL2Square();
            total++;
        }
        return new ClusterMetricsSummary(clusterId, total, distanceSum / total, distanceSquareSum, vectorNormL2Sum,
            meanVector, distance, sumVector);
    }

    public static class SaveDataAsParams extends RichMapFunction<BaseMetricsSummary, Params> {
		private static final long serialVersionUID = -7830919170205689185L;

		@Override
		public Params map(BaseMetricsSummary t) throws Exception {
			Params params = t.toMetrics().getParams();
			List <Tuple1 <Double>> silhouetteCoefficient = getRuntimeContext().getBroadcastVariable(
				EvalClusterBatchOp.SILHOUETTE_COEFFICIENT);
			params.set(ClusterMetrics.SILHOUETTE_COEFFICIENT,
				silhouetteCoefficient.get(0).f0 / params.get(ClusterMetrics.COUNT));
			return params;
		}
	}
}
