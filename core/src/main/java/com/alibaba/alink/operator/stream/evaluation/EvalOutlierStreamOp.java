package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.TimeUtil;
import com.alibaba.alink.operator.common.evaluation.BaseMetrics;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.ConfusionMatrix;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.ReplaceLabelMapFunction;
import com.alibaba.alink.operator.common.evaluation.OutlierMetricsSummary;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.evaluation.EvalOutlierStreamParams;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.INLIER_LABEL;
import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.OUTLIER_LABEL;
import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.extractPredictionScore;

/**
 * Calculate the evaluation metrics for outlier detection.
 * <p>
 * Outlier detection can be treated as a special form of binary classification, except for the predictions are always
 * fixed.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@NameCn("异常检测评估")
@NameEn("Evaluation of Outlier Detection")
public class EvalOutlierStreamOp extends StreamOperator <EvalOutlierStreamOp>
	implements EvalOutlierStreamParams <EvalOutlierStreamOp> {

	private static final long serialVersionUID = -5391026894056384871L;

	private static final String DATA_OUTPUT = "Data";

	public EvalOutlierStreamOp() {
		this(new Params());
	}

	public EvalOutlierStreamOp(Params params) {
		super(params);
	}

	/**
	 * Extract sample-wise information from data rows. Invalid data samples should be excluded.
	 *
	 * @param rows        Iterable of data rows. In every row, 1st field is the actual label, and 2nd field is the
	 *                    prediction details in JSON format.
	 * @param posValueStr The string indicating the positive label/prediction.
	 * @return A tuple of (total number of valid samples, labels of all samples, predictions of all samples, prediction
	 * scores of all samples).
	 */
	static Tuple4 <Integer, boolean[], boolean[], double[]> extractSampleInfo(Iterable <Row> rows,
																			  String posValueStr) {
		int size = Iterables.size(rows);
		boolean[] labels = new boolean[size];
		boolean[] predictions = new boolean[size];
		double[] scores = new double[size];
		int p = 0;
		for (Row row : rows) {
			String labelStr = (String) row.getField(0);
			String detailJson = (String) row.getField(1);
			if (StringUtils.isEmpty(detailJson)) {
				continue;
			}
			Tuple2 <Boolean, Double> predictionScore = extractPredictionScore(detailJson);
			boolean prediction = predictionScore.f0;
			double score = predictionScore.f1;
			labels[p] = posValueStr.equals(labelStr);
			scores[p] = score;
			predictions[p] = prediction;
			p += 1;
		}
		return Tuple4.of(p, labels, predictions, scores);
	}

	/**
	 * Extract outlier summary-related statistics.
	 *
	 * @param rows        Iterable of data rows. In every row, 1st field is the actual label, and 2nd field is the
	 *                    prediction details in JSON format.
	 * @param posValueStr The string indicating the positive label/prediction.
	 * @return a tuple of (number of valid samples, threshold-confusion matrix list in threshold-descending order,
	 * minimum score for positive predictions, maximum score of negative predictions).
	 */
	public static Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> calcOutlierStats(
		Iterable <Row> rows, String posValueStr) {
		Tuple4 <Integer, boolean[], boolean[], double[]> sampleStats = extractSampleInfo(rows, posValueStr);
		int n = sampleStats.f0;
		boolean[] labels = sampleStats.f1;
		boolean[] predictions = sampleStats.f2;
		double[] scores = sampleStats.f3;

		int numPosLabels = 0, numNegLabels = 0;
		double minPosScore = Double.MAX_VALUE, maxNegScore = Double.MIN_VALUE;
		for (int k = 0; k < n; k += 1) {
			if (labels[k]) {
				numPosLabels += 1;
			} else {
				numNegLabels += 1;
			}
			if (predictions[k]) {
				minPosScore = Math.min(minPosScore, scores[k]);
			} else {
				maxNegScore = Math.max(maxNegScore, scores[k]);
			}
		}

		int[] indices = IntStream.range(0, n)
			.boxed()
			.sorted(Comparator. <Integer, Double>comparing(d -> scores[d]).reversed())
			.mapToInt(d -> d)
			.toArray();

		List <Tuple2 <Double, ConfusionMatrix>> threshCMs = new ArrayList <>();
		long tp = 0, fp = 0;
		for (int k = 0; k < n; k += 1) {
			int index = indices[k];
			double thresh = scores[index];
			boolean label = labels[index];
			if (label) {
				tp += 1;
			} else {
				fp += 1;
			}
			ConfusionMatrix cm = new ConfusionMatrix(
				new long[][] {{tp, fp}, {numPosLabels - tp, numNegLabels - fp}});
			threshCMs.add(Tuple2.of(thresh, cm));
		}
		return Tuple4.of(n, threshCMs, minPosScore, maxNegScore);
	}

	@Override
	public EvalOutlierStreamOp linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = getLabelCol();
		String[] outlierValues = getOutlierValueStrings();
		HashSet <String> outlierValueSet = new HashSet <>(Arrays.asList(outlierValues));
		double timeInterval = getTimeInterval();

		Preconditions.checkArgument(getParams().contains(EvalOutlierStreamParams.PREDICTION_DETAIL_COL),
			"Outlier detection evaluation must give predictionDetailCol!");

		String predDetailColName = getPredictionDetailCol();
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		DataStream <Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataStream();
		data = data.map(new ReplaceLabelMapFunction(outlierValueSet, 1, 0));

		DataStream <OutlierMetricsSummary> windowsStatistics = data
			.timeWindowAll(TimeUtil.convertTime(timeInterval))
			.apply(new CalcOutlierMetricsSummaryWindowFunction(outlierValues));

		DataStream <OutlierMetricsSummary> allStatistics = windowsStatistics
			.map(new AccSummaryMapFunction <>())
			.setParallelism(1);

		DataStream <Row> windowOutput = windowsStatistics.map(
			new PrependTagMapFunction <>(ClassificationEvaluationUtil.WINDOW.f0));
		DataStream <Row> allOutput = allStatistics.map(
			new PrependTagMapFunction <>(ClassificationEvaluationUtil.ALL.f0));

		DataStream <Row> union = windowOutput.union(allOutput);

		setOutput(union,
			new String[] {ClassificationEvaluationUtil.STATISTICS_OUTPUT, DATA_OUTPUT},
			new TypeInformation[] {Types.STRING, Types.STRING});

		return this;
	}

	static class CalcOutlierMetricsSummaryWindowFunction
		implements AllWindowFunction <Row, OutlierMetricsSummary, TimeWindow> {
		private static final long serialVersionUID = 8057305974098408321L;

		private final String[] outlierValueStrings;

		public CalcOutlierMetricsSummaryWindowFunction(String[] outlierValueStrings) {
			this.outlierValueStrings = outlierValueStrings;
		}

		@Override
		public void apply(TimeWindow timeWindow, Iterable <Row> rows, Collector <OutlierMetricsSummary> collector)
			throws Exception {
			TreeSet <String> allLabels = new TreeSet <>(Collections.reverseOrder());
			for (Row row : rows) {
				allLabels.add(String.valueOf(row.getField(2)));
			}
			String[] allLabelStrs = allLabels.toArray(new String[0]);
			Object[] labelValueStrs = new Object[] {OUTLIER_LABEL, INLIER_LABEL};

			Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> outlierStatics =
				calcOutlierStats(rows, OUTLIER_LABEL);
			int n = outlierStatics.f0;
			if (0 == n) {
				return;
			}
			List <Tuple2 <Double, ConfusionMatrix>> threshCMs = outlierStatics.f1;
			double minPosScore = outlierStatics.f2;
			double maxNegScore = outlierStatics.f3;

			OutlierMetricsSummary outlierMetricsSummary = new OutlierMetricsSummary(
				n, labelValueStrs, allLabelStrs, outlierValueStrings, minPosScore, maxNegScore, threshCMs);
			collector.collect(outlierMetricsSummary);
		}
	}

	public static class AccSummaryMapFunction<M extends BaseMetricsSummary <?, M>> implements MapFunction <M, M> {
		private static final long serialVersionUID = 7732966899822527214L;
		private M acc;

		@Override
		public M map(M value) {
			acc = (null == acc ? value : acc.merge(value));
			return acc;
		}
	}

	/**
	 * Prepend tag to metrics: 'window' or 'all'
	 */
	public static class PrependTagMapFunction<T extends BaseMetrics <T>, M extends BaseMetricsSummary <T, M>>
		implements MapFunction <M, Row> {
		private static final long serialVersionUID = 7054471982821965299L;
		private final String tag;

		public PrependTagMapFunction(String tag) {
			this.tag = tag;
		}

		@Override
		public Row map(M metricsSummary) throws Exception {
			T metrics = metricsSummary.toMetrics();
			Row row = metrics.serialize();
			return Row.of(tag, row.getField(0));
		}
	}
}
