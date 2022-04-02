package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.getMiddleThresholdIndex;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setComputationsArrayParams;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setCurveAreaParams;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setCurvePointsParams;
import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setMiddleThreParams;

/**
 * Summary for outlier metrics, including a {@link BinaryMetricsSummary}, outlier labels, and all labels.
 */
public class OutlierMetricsSummary implements BaseMetricsSummary <OutlierMetrics, OutlierMetricsSummary> {

	private static final int NUM_THRESH_CMS_LIMIT = 1000;

	/**
	 * Label array.
	 */
	Object[] labels;

	/**
	 * Outlier values
	 */
	String[] outlierValueStrings;

	/**
	 * all real values
	 */
	String[] allValueStrings;

	/**
	 * Min outlier score
	 */
	double minOutlierScore;

	/**
	 * Max normal score
	 */
	double maxNormalScore;

	/**
	 * The count of samples.
	 */
	long total;

	/**
	 * Threshold-confusion matrix list, sorted by threshold ascending
	 */
	List <Tuple2 <Double, ConfusionMatrix>> threshCMs;

	public OutlierMetricsSummary(long total, Object[] labels, String[] allValueStrings, String[] outlierValueStrings,
								 double minOutlierScore, double maxNormalScore,
								 List <Tuple2 <Double, ConfusionMatrix>> threshCMs) {
		this.labels = labels;
		this.outlierValueStrings = outlierValueStrings;
		this.allValueStrings = allValueStrings;
		this.minOutlierScore = minOutlierScore;
		this.maxNormalScore = maxNormalScore;
		this.total = total;
		this.threshCMs = threshCMs;
	}

	@Override
	public OutlierMetrics toMetrics() {
		String[] labelStrs = new String[labels.length];
		for (int i = 0; i < labels.length; i++) {
			labelStrs[i] = labels[i].toString();
		}
		Params params = new Params();
		Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> threshCurves =
			extractThreshCurves(threshCMs, total);

		setCurveAreaParams(params, threshCurves.f2);

		if (threshCMs.isEmpty()) {
			params.set(BinaryClassMetrics.AUC, Double.NaN);
		}

		// TODO: sample curves

		setCurvePointsParams(params, threshCurves);
		ConfusionMatrix[] matrices = threshCurves.f0;
		setComputationsArrayParams(params, threshCurves.f1, threshCurves.f0);

		double decisionThreshold = (minOutlierScore + maxNormalScore) / 2.;
		int middleIndex = getMiddleThresholdIndex(threshCurves.f1, decisionThreshold);
		setMiddleThreParams(params, matrices[middleIndex], labelStrs);

		params.set(OutlierMetrics.OUTLIER_VALUE_ARRAY, outlierValueStrings);
		params.set(OutlierMetrics.LABEL_ARRAY, allValueStrings);
		return new OutlierMetrics(params);
	}

	/**
	 * Merge multiple threshold-confusion matrix lists in a way like multi-way merging. Only keep 1 entry in the merged
	 * list for duplicated thresholds.
	 *
	 * @param lists multiple threshold-confusion matrix lists
	 * @return merged list
	 */
	public static List <Tuple2 <Double, ConfusionMatrix>> mergeConfusionMatrixLists(
		List <Tuple2 <Double, ConfusionMatrix>>... lists) {
		//noinspection unchecked
		lists = Arrays.stream(lists)
			.filter(d -> null != d && !d.isEmpty())
			.toArray(List[]::new);
		if (0 == lists.length) {
			return new ArrayList <>();
		} else if (1 == lists.length) {
			return lists[0];
		}

		int numLists = lists.length;
		int total = Arrays.stream(lists).map(List::size).reduce(0, Integer::sum);

		List <Tuple2 <Double, ConfusionMatrix>> merged = new ArrayList <>();
		// Pointer to current element in the list
		int[] p = new int[numLists];
		// Current threshold for comparison
		double[] cntThresh = new double[numLists];
		// Stores the last confusion matrix whose corresponding threshold is above current one
		ConfusionMatrix[] prevCms = new ConfusionMatrix[numLists];
		for (int i = 0; i < numLists; i += 1) {
			cntThresh[i] = lists[i].get(p[i]).f0;
			prevCms[i] = new ConfusionMatrix(new long[][] {{0, 0}, lists[i].get(0).f1.getActualLabelFrequency()});
		}

		for (int k = 0; k < total; k += 1) {
			boolean hasEqualThresh = false;

			boolean init = false;
			double maxThresh = Double.NEGATIVE_INFINITY;
			int maxThreshId = 0;

			for (int i = 0; i < numLists; i += 1) {
				if (p[i] >= lists[i].size()) {
					continue;
				}
				if (!init || (cntThresh[i] > maxThresh)) {
					maxThresh = cntThresh[i];
					maxThreshId = i;
					init = true;
				} else if (cntThresh[i] == maxThresh) {
					hasEqualThresh = true;
				}
			}
			prevCms[maxThreshId] = lists[maxThreshId].get(p[maxThreshId]).f1;

			// If some lists have same thresholds, only add the last one
			if (!hasEqualThresh) {
				LongMatrix longMatrix = new LongMatrix(new long[2][2]);
				for (int i = 0; i < numLists; i += 1) {
					longMatrix.plusEqual(prevCms[i].longMatrix);
				}
				merged.add(Tuple2.of(maxThresh, new ConfusionMatrix(longMatrix)));
			}
			p[maxThreshId] += 1;
			cntThresh[maxThreshId] = p[maxThreshId] < lists[maxThreshId].size()
				? lists[maxThreshId].get(p[maxThreshId]).f0
				: Double.NEGATIVE_INFINITY;
		}
		return merged;
	}

	/**
	 * Remove some entries from list, so its size is less than or equal to limit.
	 * <p>
	 * Iteratively remove the entry with the smallest threshold difference to its predecessor, until the limit met.
	 *
	 * @param threshCMs threshold-confusion matrix list
	 * @param limit     size limit
	 * @return filtered list
	 */
	public static List <Tuple2 <Double, ConfusionMatrix>> filterCloseEntries(
		List <Tuple2 <Double, ConfusionMatrix>> threshCMs, int limit) {
		int n = threshCMs.size();
		if (n <= limit) {
			return threshCMs;
		}
		int toRemove = n - limit;

		// Maintain a double-linked list to store the prev/next entry when removing
		int[] prev = new int[n];
		int[] next = new int[n];
		boolean[] removed = new boolean[n];
		for (int i = 0; i < n; i += 1) {
			prev[i] = i - 1;
			next[i] = i + 1;
			removed[i] = false;
		}

		// Use a priority queue to help extract the entry with the smallest threshold difference compared to its previous entry.
		// The tuple contains current index and its previous index.
		// If multiple entries have the same smallest value, pick the one with the smallest index.
		Comparator <Tuple2 <Integer, Integer>> comparator = Comparator
			. <Tuple2 <Integer, Integer>, Double>comparing(d -> threshCMs.get(d.f1).f0 - threshCMs.get(d.f0).f0)
			.thenComparingInt(d -> d.f0);
		PriorityQueue <Tuple2 <Integer, Integer>> pq = new PriorityQueue <>(comparator);
		for (int i = 1; i < n; i += 1) {
			pq.add(Tuple2.of(i, i - 1));
		}

		while (toRemove > 0) {
			Tuple2 <Integer, Integer> cntTuple = pq.remove();
			// Tuples with same current index could be added multiple times after removal,
			// so need to check its current predecessor.
			if (cntTuple.f1 != prev[cntTuple.f0]) {
				continue;
			}
			int cntIndex = cntTuple.f0;
			int prevIndex = cntTuple.f1;
			int nextIndex = next[cntIndex];
			if (nextIndex < n) {
				prev[nextIndex] = prevIndex;
			}
			if (prevIndex >= 0) {
				next[prevIndex] = nextIndex;
			}
			if (nextIndex < n) {
				pq.add(Tuple2.of(nextIndex, prevIndex));
			}
			removed[cntIndex] = true;
			toRemove -= 1;
		}

		return IntStream.range(0, n)
			.filter(d -> !removed[d])
			.mapToObj(threshCMs::get)
			.collect(Collectors.toList());
	}

	@Override
	public OutlierMetricsSummary merge(OutlierMetricsSummary other) {
		if (null == other) {
			return this;
		}
		Preconditions.checkState(Arrays.equals(labels, other.labels), "The labels are not the same!");

		total += other.total;

		TreeSet <String> mergedOutlierValues = new TreeSet <>(Comparator.reverseOrder());
		mergedOutlierValues.addAll(Arrays.asList(allValueStrings));
		mergedOutlierValues.addAll(Arrays.asList(other.allValueStrings));
		allValueStrings = mergedOutlierValues.toArray(new String[0]);

		//noinspection unchecked
		threshCMs = filterCloseEntries(mergeConfusionMatrixLists(threshCMs, other.threshCMs), NUM_THRESH_CMS_LIMIT);
		return this;
	}

	static Tuple3 <ConfusionMatrix[], double[], EvaluationCurve[]> extractThreshCurves(
		List <Tuple2 <Double, ConfusionMatrix>> threshCMs, long total) {

		int n = threshCMs.size();
		ConfusionMatrix lastCM = threshCMs.get(n - 1).f1;
		long[] actualLabelFrequency = lastCM.getActualLabelFrequency();
		long totalTrue = actualLabelFrequency[0];
		long totalFalse = actualLabelFrequency[1];

		Preconditions.checkState(totalFalse + totalTrue == total,
			"The effective number in bins must be equal to total!");

		EvaluationCurvePoint[] rocCurve = new EvaluationCurvePoint[n + 1];
		EvaluationCurvePoint[] recallPrecisionCurve = new EvaluationCurvePoint[n + 1];
		EvaluationCurvePoint[] liftChart = new EvaluationCurvePoint[n + 1];
		EvaluationCurvePoint[] lorenzCurve = new EvaluationCurvePoint[n + 1];
		ConfusionMatrix[] data = new ConfusionMatrix[n + 1];
		double[] threshold = new double[n + 1];

		// Iterate all threshold-confusion matrix in reverse order (threshold descending order)
		for (int k = 1; k <= n; k += 1) {
			double thresh = threshCMs.get(k - 1).f0;
			ConfusionMatrix cm = threshCMs.get(k - 1).f1;
			long curTrue = cm.longMatrix.getValue(0, 0);
			long curFalse = cm.longMatrix.getValue(0, 1);
			threshold[k] = thresh;
			data[k] = cm;

			double tpr = totalTrue == 0 ? 1. : 1. * curTrue / totalTrue;
			double fpr = totalFalse == 0 ? 1. : 1. * curFalse / totalFalse;
			double precision = curTrue + curFalse == 0 ? 1. : 1. * curTrue / (curTrue + curFalse);
			double pr = 1. * (curTrue + curFalse) / total;

			rocCurve[k] = new EvaluationCurvePoint(fpr, tpr, threshold[k]);
			recallPrecisionCurve[k] = new EvaluationCurvePoint(tpr, precision, threshold[k]);
			liftChart[k] = new EvaluationCurvePoint(pr, curTrue, threshold[k]);
			lorenzCurve[k] = new EvaluationCurvePoint(pr, tpr, threshold[k]);
		}

		threshold[0] = 1.;
		data[0] = new ConfusionMatrix(new long[][] {{0, 0}, {totalTrue, totalFalse}});

		rocCurve[0] = new EvaluationCurvePoint(0, 0, threshold[0]);
		recallPrecisionCurve[0] = new EvaluationCurvePoint(0, recallPrecisionCurve[1].getY(), threshold[0]);
		liftChart[0] = new EvaluationCurvePoint(0, 0, threshold[0]);
		lorenzCurve[0] = new EvaluationCurvePoint(0, 0, threshold[0]);

		return Tuple3.of(data, threshold, new EvaluationCurve[] {new EvaluationCurve(rocCurve),
			new EvaluationCurve(recallPrecisionCurve), new EvaluationCurve(liftChart),
			new EvaluationCurve(lorenzCurve)});
	}

	public Object[] getLabels() {
		return labels;
	}

	public String[] getOutlierValueStrings() {
		return outlierValueStrings;
	}

	public String[] getAllValueStrings() {
		return allValueStrings;
	}

	public double getMinOutlierScore() {
		return minOutlierScore;
	}

	public double getMaxNormalScore() {
		return maxNormalScore;
	}

	public long getTotal() {
		return total;
	}

	public List <Tuple2 <Double, ConfusionMatrix>> getThreshCMs() {
		return threshCMs;
	}
}
