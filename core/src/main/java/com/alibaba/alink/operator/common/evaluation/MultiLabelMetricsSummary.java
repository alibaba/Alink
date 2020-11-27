package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

public final class MultiLabelMetricsSummary
	implements BaseMetricsSummary <MultiLabelMetrics, MultiLabelMetricsSummary> {
	private static final long serialVersionUID = -5625413134249673912L;
	/**
	 * Precision for a single sample.
	 */
	double precision;

	/**
	 * Recall for a single sample.
	 */
	double recall;

	/**
	 * Accuracy for a single sample
	 */
	double accuracy;

	/**
	 * F1 measure for a single sample.
	 */
	double f1;

	/**
	 * The count of samples.
	 */
	long total;

	/**
	 * Label Number.
	 */
	long labelNumber;

	/**
	 * Same subset.
	 */
	long numSameSubSets;

	/**
	 * Hit rate.
	 */

	///**
	// * Labels.
	// */
	//Object[] labels;
	//
	//HashMap<Object, Long> labelIntersect;
	//HashMap<Object, Long> labelActual;
	//HashMap<Object, Long> labelPred;

	/**
	 * ConfusionMatrix, tp = |Pi ∩ Li|, fp = |Pi - Li|, fn = |Li - Pi|, Li is the label set, Pi is the prediction set.
	 */
	LongMatrix longMatrix;

	public MultiLabelMetricsSummary(int intersect, int sampleLabelNum, int predictionNum, int labelNumber) {
		int tp = intersect;
		int fp = predictionNum - intersect;
		int fn = sampleLabelNum - intersect;
		this.precision = (predictionNum == 0 ? 0.0 : intersect * 1.0 / predictionNum);
		this.recall = (sampleLabelNum == 0 ? 0.0 : intersect * 1.0 / sampleLabelNum);
		this.accuracy = tp * 1.0 / (tp + fp + fn);
		this.f1 = 2.0 * tp / (2 * tp + fp + fn);
		this.longMatrix = new LongMatrix(new long[][] {{(long) tp, (long) fp}, {(long) fn, 0L}});
		this.total = 1L;
		this.labelNumber = labelNumber;
		if (intersect == sampleLabelNum && sampleLabelNum == predictionNum) {
			numSameSubSets = 1;
		} else {
			numSameSubSets = 0;
		}
		//this.labels = labels;
		//this.labelActual = labelActual;
		//this.labelPred = labelPred;
		//this.labelIntersect = labelIntersect;
	}

	/**
	 * Add the precision, recall, accuracy.
	 *
	 * @param multiLabelMetrics the MultiLabelSummary to merge.
	 * @return the merged result.
	 */
	@Override
	public MultiLabelMetricsSummary merge(MultiLabelMetricsSummary multiLabelMetrics) {
		if (null == multiLabelMetrics) {
			return this;
		}
		Preconditions.checkArgument(this.labelNumber == multiLabelMetrics.labelNumber, "LabelNumber not equal!");
		this.precision += multiLabelMetrics.precision;
		this.recall += multiLabelMetrics.recall;
		this.accuracy += multiLabelMetrics.accuracy;
		this.total += multiLabelMetrics.total;
		this.f1 += multiLabelMetrics.f1;
		this.longMatrix.plusEqual(multiLabelMetrics.longMatrix);
		this.numSameSubSets += multiLabelMetrics.numSameSubSets;
		//if(this.labelIntersect == null){
		//    this.labelIntersect = multiLabelMetrics.labelIntersect;
		//    this.labelPred = multiLabelMetrics.labelPred;
		//    this.labelActual = multiLabelMetrics.labelActual;
		//}else{
		//    for(Map.Entry<Object, Long> entry : multiLabelMetrics.labelIntersect.entrySet()){
		//        this.labelIntersect.merge(entry.getKey(), entry.getValue(), (v1, v2) -> (v1 + v2));
		//    }
		//    for(Map.Entry<Object, Long> entry : multiLabelMetrics.labelActual.entrySet()){
		//        this.labelActual.merge(entry.getKey(), entry.getValue(), (v1, v2) -> (v1 + v2));
		//    }
		//    for(Map.Entry<Object, Long> entry : multiLabelMetrics.labelPred.entrySet()){
		//        this.labelPred.merge(entry.getKey(), entry.getValue(), (v1, v2) -> (v1 + v2));
		//    }
		//}
		return this;
	}

	/**
	 * Calculate the detail info based on the confusion matrix.
	 */
	@Override
	public MultiLabelMetrics toMetrics() {
		Params params = new Params();
		int tp = (int) longMatrix.getValue(0, 0);
		int fp = (int) longMatrix.getValue(0, 1);
		int fn = (int) longMatrix.getValue(1, 0);
		params.set(MultiLabelMetrics.PRECISION, precision / total);
		params.set(MultiLabelMetrics.RECALL, recall / total);
		params.set(MultiLabelMetrics.F1, f1 / total);
		params.set(MultiLabelMetrics.ACCURACY, accuracy / total);
		params.set(MultiLabelMetrics.MICRO_PRECISION, 1. * tp / (tp + fp));
		params.set(MultiLabelMetrics.MICRO_RECALL, tp * 1.0 / (tp + fn));
		params.set(MultiLabelMetrics.MICRO_F1, 2.0 * tp / (2 * tp + fp + fn));
		params.set(MultiLabelMetrics.HAMMING_LOSS, (fp + fn) * 1. / total / labelNumber);
		params.set(MultiLabelMetrics.SUBSET_ACCURACY, numSameSubSets * 1. / total);
		return new MultiLabelMetrics(params);
	}
}
