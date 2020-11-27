package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setLoglossParams;

/**
 * Save the evaluation data for binary classification.
 */
public final class AccurateBinaryMetricsSummary
	implements BaseMetricsSummary <BinaryClassMetrics, AccurateBinaryMetricsSummary> {
	private static final long serialVersionUID = 4614108912380382179L;

	/**
	 * Label array.
	 */
	Object[] labels;

	/**
	 * The count of samples.
	 */
	long total;

	/**
	 * Area under Roc
	 */
	double auc;

	/**
	 * Area under Lorenz
	 */
	double gini;

	/**
	 * Area under PRC
	 */
	double prc;

	/**
	 * KS
	 */
	double ks;

	/**
	 * Logloss = sum_i{sum_j{y_ij * log(p_ij)}}
	 */
	double logLoss;

	List <Tuple2 <Double, ConfusionMatrix>> metricsInfoList;

	public AccurateBinaryMetricsSummary(Object[] labels, double logLoss, long total, double auc) {
		this.labels = labels;
		this.logLoss = logLoss;
		this.total = total;
		this.auc = auc;
		metricsInfoList = new ArrayList <>();
	}

	/**
	 * Merge the bins, and add the logLoss.
	 *
	 * @param binaryClassMetrics the BinaryMetricsSummary to merge.
	 * @return the merged metrics.
	 */
	@Override
	public AccurateBinaryMetricsSummary merge(AccurateBinaryMetricsSummary binaryClassMetrics) {
		if (null == binaryClassMetrics) {
			return this;
		}
		Preconditions.checkState(Arrays.equals(labels, binaryClassMetrics.labels), "The labels are not the same!");
		Preconditions.checkState(Double.compare(this.auc, binaryClassMetrics.auc) == 0, "Auc not equal!");

		this.logLoss += binaryClassMetrics.logLoss;
		this.total += binaryClassMetrics.total;
		this.ks = Math.max(this.ks, binaryClassMetrics.ks);
		this.prc += binaryClassMetrics.prc;
		this.gini += binaryClassMetrics.gini;

		metricsInfoList.addAll(binaryClassMetrics.metricsInfoList);
		return this;
	}

	/**
	 * Calculate the detail info based on the bins and save them into params.
	 *
	 * @return BinaryClassMetrics.
	 */
	@Override
	public BinaryClassMetrics toMetrics() {
		metricsInfoList.sort(Comparator.comparingDouble(v -> -v.f0));
		String[] labelStrs = new String[labels.length];
		for (int i = 0; i < labels.length; i++) {
			labelStrs[i] = labels[i].toString();
		}
		Params params = new Params();
		setCurveAreaParams(params, auc, ks, prc, gini);

		ConfusionMatrix[] matrices = new ConfusionMatrix[metricsInfoList.size()];
		double[] thresholds = new double[metricsInfoList.size()];
		for (int i = 0; i < metricsInfoList.size(); i++) {
			thresholds[i] = metricsInfoList.get(i).f0;
			matrices[i] = metricsInfoList.get(i).f1;
		}
		setCurvePointsParams(params, thresholds, matrices);

		BinaryMetricsSummary.setComputationsArrayParams(params, thresholds, matrices);
		setLoglossParams(params, logLoss, total);
		int middleIndex = BinaryMetricsSummary.getMiddleThresholdIndex(thresholds);
		BinaryMetricsSummary.setMiddleThreParams(params, matrices[middleIndex], labelStrs);
		return new BinaryClassMetrics(params);
	}

	private static void setCurveAreaParams(Params params, double auc, double ks, double prc, double gini) {
		params.set(BinaryClassMetrics.AUC, auc);
		params.set(BinaryClassMetrics.PRC, prc);
		params.set(BinaryClassMetrics.KS, ks);
		params.set(BinaryClassMetrics.GINI, (gini - 0.5) / 0.5);
	}

	private static void setCurvePointsParams(Params params,
											 double[] thresholds,
											 ConfusionMatrix[] confusionMatrices) {
		long totalTrue;
		long totalFalse;
		if (thresholds.length > 0) {
			ConfusionMatrix confusionMatrix = confusionMatrices[0];
			totalTrue = confusionMatrix.getActualLabelFrequency()[0];
			totalFalse = confusionMatrix.getActualLabelFrequency()[1];
		} else {
			return;
		}
		EvaluationCurvePoint[] rocCurve = new EvaluationCurvePoint[confusionMatrices.length];
		EvaluationCurvePoint[] recallPrecisionCurve = new EvaluationCurvePoint[confusionMatrices.length];
		EvaluationCurvePoint[] liftChart = new EvaluationCurvePoint[confusionMatrices.length];
		EvaluationCurvePoint[] lorenzCurve = new EvaluationCurvePoint[confusionMatrices.length];
		long total = totalTrue + totalFalse;
		for (int i = 0; i < confusionMatrices.length; i++) {
			double threshold = thresholds[i];
			ConfusionMatrix confusionMatrix = confusionMatrices[i];
			long curTrue = confusionMatrix.longMatrix.getValue(0, 0);
			long curFalse = confusionMatrix.longMatrix.getValue(0, 1);
			double precision = (Double.compare(threshold, 1.0) == 0 && i >= 1 ? recallPrecisionCurve[i - 1].getY() :
				curTrue + curFalse == 0 ? 1.0 : 1.0 * curTrue / (curTrue + curFalse));
			double tpr = (totalTrue == 0 ? 1.0 : 1.0 * curTrue / totalTrue);
			double fpr = (totalFalse == 0 ? 1.0 : 1.0 * curFalse / totalFalse);
			double pr = 1.0 * (curTrue + curFalse) / total;
			rocCurve[i] = new EvaluationCurvePoint(fpr, tpr, threshold);
			recallPrecisionCurve[i] = new EvaluationCurvePoint(tpr, precision, threshold);
			liftChart[i] = new EvaluationCurvePoint(pr, curTrue, threshold);
			lorenzCurve[i] = new EvaluationCurvePoint(pr, tpr, threshold);
		}
		params.set(BinaryClassMetrics.ROC_CURVE, new EvaluationCurve(rocCurve).getXYArray());
		params.set(BinaryClassMetrics.RECALL_PRECISION_CURVE, new EvaluationCurve(recallPrecisionCurve).getXYArray());
		params.set(BinaryClassMetrics.LIFT_CHART, new EvaluationCurve(liftChart).getXYArray());
		params.set(BinaryClassMetrics.LORENZ_CURVE, new EvaluationCurve(lorenzCurve).getXYArray());
	}
}
