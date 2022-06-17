package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setClassificationCommonParams;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.setLoglossParams;

/**
 * Save the evaluation data for binary classification.
 */
public final class BinaryMetricsSummary
	implements BaseMetricsSummary <BinaryClassMetrics, BinaryMetricsSummary> {
	private static final long serialVersionUID = 4614108912380382179L;
	/**
	 * The minimum interval of probabilities for output. When calculating auc/prc/ks, we need as many bins as possible
	 * to improve the accuracy. While outputting the indexes, we should sample the data.
	 */
	private static double PROBABILITY_INTERVAL = 0.001;

	/**
	 * Allow the calculation error.
	 */
	private static double PROBABILITY_ERROR = 0.00001;

	/**
	 * Label array.
	 */
	Object[] labels;

	/**
	 * The count of samples.
	 */
	long total;

	/**
	 * Divide [0,1] into <code>ClassificationEvaluationUtil.DETAIL_BIN_NUMBER</code> bins. If the probability of the
	 * positive value of a sample is p and the corresponding bin index is p * <code>ClassificationEvaluationUtil
	 * .DETAIL_BIN_NUMBER</code>. If the label of the sample is positive, then
	 * <code>positiveBin[index]++</code>, otherwise, <code>negativeBin[index]++</code>.
	 */
	long[] positiveBin, negativeBin;

	private double decisionThreshold;
	/**
	 * Logloss = sum_i{sum_j{y_ij * log(p_ij)}}
	 */
	double logLoss;

	public BinaryMetricsSummary() {
	}

	public BinaryMetricsSummary(long[] positiveBin, long[] negativeBin, Object[] labels, double logLoss,
								long total) {
		this(positiveBin, negativeBin, labels, 0.5, logLoss, total);
	}

	public BinaryMetricsSummary(long[] positiveBin, long[] negativeBin, Object[] labels, double decisionThreshold,
								double logLoss, long total) {
		this.positiveBin = positiveBin;
		this.negativeBin = negativeBin;
		this.labels = labels;
		this.decisionThreshold = decisionThreshold;
		this.logLoss = logLoss;
		this.total = total;
	}

	/**
	 * Merge the bins, and add the logLoss.
	 *
	 * @param binaryClassMetrics the BinaryMetricsSummary to merge.
	 * @return the merged metrics.
	 */
	@Override
	public BinaryMetricsSummary merge(BinaryMetricsSummary binaryClassMetrics) {
		if (null == binaryClassMetrics) {
			return this;
		}
		Preconditions.checkState(Arrays.equals(labels, binaryClassMetrics.labels), "The labels are not the same!");

		for (int i = 0; i < positiveBin.length; i++) {
			positiveBin[i] += binaryClassMetrics.positiveBin[i];
		}
		for (int i = 0; i < negativeBin.length; i++) {
			negativeBin[i] += binaryClassMetrics.negativeBin[i];
		}
		logLoss += binaryClassMetrics.logLoss;
		total += binaryClassMetrics.total;
		return this;
	}

	/**
	 * Calculate the detail info based on the bins and save them into params.
	 *
	 * @return BinaryClassMetrics.
	 */
	@Override
	public BinaryClassMetrics toMetrics() {
		String[] labelStrs = new String[labels.length];
		for (int i = 0; i < labels.length; i++) {
			labelStrs[i] = labels[i].toString();
		}
		Params params = new Params();
		Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> matrixThreCurve =
			extractMatrixThreCurve(positiveBin, negativeBin, total);

		setCurveAreaParams(params, matrixThreCurve.f2);

		if(Arrays.stream(negativeBin).sum() == 0 || Arrays.stream(positiveBin).sum() == 0){
			params.set(BinaryClassMetrics.AUC, Double.NaN);
		}

		Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> sampledMatrixThreCurve = sample(
			PROBABILITY_INTERVAL, matrixThreCurve);

		setCurvePointsParams(params, sampledMatrixThreCurve);
		ConfusionMatrix[] matrices = sampledMatrixThreCurve.f0;
		setComputationsArrayParams(params, sampledMatrixThreCurve.f1, sampledMatrixThreCurve.f0);
		setLoglossParams(params, logLoss, total);
		int middleIndex = getMiddleThresholdIndex(sampledMatrixThreCurve.f1, decisionThreshold);
		setMiddleThreParams(params, matrices[middleIndex], labelStrs);
		return new BinaryClassMetrics(params);
	}

	/**
	 * Some metrics are only given at the middle threshold.
	 *
	 * @param params          Params.
	 * @param confusionMatrix ConfusionMatrix.
	 * @param labels          label array.
	 */
	public static void setMiddleThreParams(Params params, ConfusionMatrix confusionMatrix, String[] labels) {
		params.set(BinaryClassMetrics.PRECISION,
			ClassificationEvaluationUtil.Computations.PRECISION.computer.apply(confusionMatrix, 0));
		params.set(BinaryClassMetrics.RECALL,
			ClassificationEvaluationUtil.Computations.RECALL.computer.apply(confusionMatrix, 0));
		params.set(BinaryClassMetrics.F1,
			ClassificationEvaluationUtil.Computations.F1.computer.apply(confusionMatrix, 0));
		setClassificationCommonParams(params, confusionMatrix, labels);
	}

	/**
	 * Set the RocCurve/PrecisionRecallCurve/LiftChar.
	 *
	 * @param params                 Params.
	 * @param sampledMatrixThreCurve sampled data.
	 */
	static void setCurvePointsParams(Params params,
									 Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> sampledMatrixThreCurve) {
		params.set(BinaryClassMetrics.ROC_CURVE, sampledMatrixThreCurve.f2[0].getXYArray());
		params.set(BinaryClassMetrics.PRECISION_RECALL_CURVE, sampledMatrixThreCurve.f2[1].getXYArray());
		params.set(BinaryClassMetrics.LIFT_CHART, sampledMatrixThreCurve.f2[2].getXYArray());
		params.set(BinaryClassMetrics.LORENZ_CURVE, sampledMatrixThreCurve.f2[3].getXYArray());
	}

	/**
	 * To get more accurate AUC/PRC/KS, we calculate them before sampling.
	 *
	 * @param params Params.
	 * @param curves Array of ConfusionMatrix/threshold/Curves.
	 */
	static void setCurveAreaParams(Params params, EvaluationCurve[] curves) {
		params.set(BinaryClassMetrics.AUC, curves[0].calcArea());
		params.set(BinaryClassMetrics.PRC, curves[1].calcArea());
		params.set(BinaryClassMetrics.KS, curves[0].calcKs());
		params.set(BinaryClassMetrics.GINI, (curves[3].calcArea() - 0.5) / 0.5);
	}

	/**
	 * Set all the metrics of the positive label.
	 *
	 * @param params   Params.
	 * @param matrices ConfusionMatrix array.
	 */
	static void setComputationsArrayParams(Params params, double[] thresholdArray, ConfusionMatrix[] matrices) {
		params.set(BinaryClassMetrics.THRESHOLD_ARRAY, thresholdArray);
		double[][] paramData = new double[ClassificationEvaluationUtil.Computations.values().length][matrices.length];
		for (int i = 0; i < matrices.length; i++) {
			for (ClassificationEvaluationUtil.Computations c : ClassificationEvaluationUtil.Computations.values()) {
				paramData[c.ordinal()][i] = c.computer.apply(matrices[i], 0);
			}
		}
		for (ClassificationEvaluationUtil.Computations c : ClassificationEvaluationUtil.Computations.values()) {
			params.set(c.arrayParamInfo, paramData[c.ordinal()]);
		}
	}

	/**
	 * Extract the bins who are not empty, keep the middle threshold 0.5.
	 * <p>
	 * Initialize the RocCurve, Recall-Precision Curve and Lift Curve.
	 * <p>
	 * RocCurve: (FPR, TPR), starts with (0,0). Recall-Precision Curve: (recall, precision), starts with (0, p), p is
	 * the precision with the lowest. LiftChart: (TP+FP/total, TP), starts with (0,0). confusion matrix = [TP FP][FN
	 * TN].
	 *
	 * @param positiveBin positiveBins.
	 * @param negativeBin negativeBins.
	 * @param total       sample number
	 * @return ConfusionMatrix array, threshold array, rocCurve/PrecisionRecallCurve/LiftChart.
	 */
	static Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> extractMatrixThreCurve(long[] positiveBin,
																						  long[] negativeBin,
																						  long total) {
		ArrayList <Integer> effectiveIndices = new ArrayList <>();
		long totalTrue = 0, totalFalse = 0;
		for (int i = 0; i < ClassificationEvaluationUtil.DETAIL_BIN_NUMBER; i++) {
			if (0L != positiveBin[i] || 0L != negativeBin[i]
				|| i == ClassificationEvaluationUtil.DETAIL_BIN_NUMBER / 2) {
				effectiveIndices.add(i);
				totalTrue += positiveBin[i];
				totalFalse += negativeBin[i];
			}
		}
		Preconditions.checkState(totalFalse + totalTrue == total,
			"The effective number in bins must be equal to total!");

		final int length = effectiveIndices.size();
		final int newLen = length + 1;
		final double m = 1.0 / ClassificationEvaluationUtil.DETAIL_BIN_NUMBER;
		EvaluationCurvePoint[] rocCurve = new EvaluationCurvePoint[newLen];
		EvaluationCurvePoint[] precisionRecallCurve = new EvaluationCurvePoint[newLen];
		EvaluationCurvePoint[] liftChart = new EvaluationCurvePoint[newLen];
		EvaluationCurvePoint[] lorenzCurve = new EvaluationCurvePoint[newLen];
		ConfusionMatrix[] data = new ConfusionMatrix[newLen];
		double[] threshold = new double[newLen];
		long curTrue = 0;
		long curFalse = 0;
		for (int i = 1; i < newLen; i++) {
			int index = effectiveIndices.get(length - i);
			curTrue += positiveBin[index];
			curFalse += negativeBin[index];
			threshold[i] = index * m;
			data[i] = new ConfusionMatrix(
				new long[][] {{curTrue, curFalse}, {totalTrue - curTrue, totalFalse - curFalse}});
			double tpr = (totalTrue == 0 ? 1.0 : 1.0 * curTrue / totalTrue);
			double fpr = (totalFalse == 0 ? 1.0 : 1.0 * curFalse / totalFalse);
			double precision = curTrue + curTrue == 0 ? 1.0 : 1.0 * curTrue / (curTrue + curFalse);
			double pr = 1.0 * (curTrue + curFalse) / total;
			rocCurve[i] = new EvaluationCurvePoint(fpr, tpr, threshold[i]);
			precisionRecallCurve[i] = new EvaluationCurvePoint(tpr, precision, threshold[i]);
			liftChart[i] = new EvaluationCurvePoint(pr, curTrue, threshold[i]);
			lorenzCurve[i] = new EvaluationCurvePoint(pr, tpr, threshold[i]);
		}

		threshold[0] = 1.0;
		data[0] = new ConfusionMatrix(new long[][] {{0, 0}, {totalTrue, totalFalse}});
		rocCurve[0] = new EvaluationCurvePoint(0, 0, threshold[0]);
		precisionRecallCurve[0] = new EvaluationCurvePoint(0, precisionRecallCurve[1].getY(), threshold[0]);
		liftChart[0] = new EvaluationCurvePoint(0, 0, threshold[0]);
		lorenzCurve[0] = new EvaluationCurvePoint(0, 0, threshold[0]);

		return Tuple3.of(data, threshold, new EvaluationCurve[] {new EvaluationCurve(rocCurve),
			new EvaluationCurve(precisionRecallCurve), new EvaluationCurve(liftChart),
			new EvaluationCurve(lorenzCurve)});
	}

	/**
	 * Pick the middle point where threshold is 0.5.
	 *
	 * @param threshold threshold array.
	 * @return the middle index.
	 */
	static int getMiddleThresholdIndex(double[] threshold) {
		return getMiddleThresholdIndex(threshold, 0.5);
	}

	static int getMiddleThresholdIndex(double[] threshold, double decisionThreshold) {
		double min = Double.MAX_VALUE;
		int index = 0;
		for (int i = 0; i < threshold.length; i++) {
			if (Math.abs(threshold[i] - decisionThreshold) < min) {
				min = Math.abs(threshold[i] - decisionThreshold);
				index = i;
			}
		}
		return index;
	}

	/**
	 * Sample the output points.
	 *
	 * @param eps      sample interval.
	 * @param fullData original data.
	 * @return sampled data.
	 */
	static Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> sample(double eps,
																		  Tuple3 <ConfusionMatrix[], double[],
																			  EvaluationCurve[]> fullData) {
		List <Integer> reservedData = new ArrayList <>();
		reservedData.add(0);

		ConfusionMatrix[] data = fullData.f0;
		double[] p = fullData.f1;
		EvaluationCurve[] curves = fullData.f2;
		double preThre = p[0];

		for (int i = 0; i < p.length; i++) {
			if (Math.abs(preThre - p[i]) >= eps - PROBABILITY_ERROR || Math.abs(p[i] - 0.5) < PROBABILITY_ERROR) {
				reservedData.add(i);
				preThre = p[i];
			}
		}

		//remove the threshold at 1.0.
		double[] sampledP = new double[reservedData.size() - 1];
		ConfusionMatrix[] sampledData = new ConfusionMatrix[reservedData.size() - 1];
		EvaluationCurvePoint[][] sampledCurvesPoints = new EvaluationCurvePoint[curves.length][reservedData.size()];

		for (int i = 0; i < reservedData.size(); i++) {
			if (i > 0) {
				sampledP[i - 1] = p[reservedData.get(i)];
				sampledData[i - 1] = data[reservedData.get(i)];
			}
			for (int j = 0; j < curves.length; j++) {
				sampledCurvesPoints[j][i] = curves[j].getPoints()[reservedData.get(i)];
			}
		}

		EvaluationCurve[] newCurves = new EvaluationCurve[curves.length];
		for (int i = 0; i < curves.length; i++) {
			newCurves[i] = new EvaluationCurve(sampledCurvesPoints[i]);
		}
		return Tuple3.of(sampledData, sampledP, newCurves);
	}
}
