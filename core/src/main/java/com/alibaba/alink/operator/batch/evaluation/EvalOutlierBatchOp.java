package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.AccurateBinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.ConfusionMatrix;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.ProbMapExtractor;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.ReplaceLabelMapFunction;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.operator.common.outlier.OutlierDetector;
import com.alibaba.alink.operator.common.utils.OutlierMetricsHtmlVisualizer;
import com.alibaba.alink.params.evaluation.EvalOutlierParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setMiddleThreParams;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.LABELS_BC_NAME;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.calLabelPredDetailLocal;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.calcSampleStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.LABEL_INFO;
import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.extractPredictionScore;

/**
 * Calculate the evaluation metrics for outlier detection.
 * <p>
 * Outlier detection can be treated as a special form of binary classification, except for the predictions are always
 * fixed.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionDetailCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("异常检测评估")
@NameEn("Evaluation of Outlier Detection")
public class EvalOutlierBatchOp extends BatchOperator <EvalOutlierBatchOp> implements
	EvalOutlierParams <EvalOutlierBatchOp>,
	EvaluationMetricsCollector <OutlierMetrics, EvalOutlierBatchOp> {

	private static final long serialVersionUID = 2810628840286798959L;
	private static final Logger LOG = LoggerFactory.getLogger(EvalOutlierBatchOp.class);
	private static final String PRED_BASED_CM = "pred_based_cm";

	public EvalOutlierBatchOp() {
		this(null);
	}

	public EvalOutlierBatchOp(Params params) {
		super(params);
	}

	/**
	 * Obtain all real labels, used in metrics.
	 */
	private static DataSet <String> calcRealLabels(DataSet <Row> data) {
		return data.map(new MapFunction <Row, String>() {
			@Override
			public String map(Row value) {
				return value.getField(0).toString();
			}
		}).distinct();
	}

	/**
	 * Estimate decision threshold from outlier scores and predictions.
	 * <p>
	 * Here, the average of minimum score of outlier samples and maximum score of normal samples is used.
	 *
	 * @return
	 */
	private static DataSet <Double> calcDecisionThreshold(DataSet <Row> data) {
		return data.rebalance()
			.mapPartition(new MapPartitionFunction <Row, Tuple4 <Double, Double, Double, Double>>() {
				@Override
				public void mapPartition(Iterable <Row> values,
										 Collector <Tuple4 <Double, Double, Double, Double>> out) {
					Tuple4 <Double, Double, Double, Double> scoreBoundary =
						Tuple4.of(Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE);
					for (Row value : values) {
						String detail = (String) value.getField(1);
						if (StringUtils.isEmpty(detail)) {
							continue;
						}
						Tuple2 <Boolean, Double> predictionAndScore = extractPredictionScore(detail);
						boolean isOutlier = predictionAndScore.f0;
						double score = predictionAndScore.f1;
						scoreBoundary.f0 = Math.min(scoreBoundary.f0, score);
						scoreBoundary.f1 = Math.max(scoreBoundary.f1, score);
						if (isOutlier) {
							scoreBoundary.f2 = Math.min(scoreBoundary.f2, score);
						} else {
							scoreBoundary.f3 = Math.max(scoreBoundary.f3, score);
						}
					}
					out.collect(scoreBoundary);
				}
			})
			.reduce(new ReduceFunction <Tuple4 <Double, Double, Double, Double>>() {
				@Override
				public Tuple4 <Double, Double, Double, Double> reduce(Tuple4 <Double, Double, Double, Double> value1,
																	  Tuple4 <Double, Double, Double, Double> value2) {
					return Tuple4.of(
						Math.min(value1.f0, value2.f0),
						Math.max(value1.f1, value2.f1),
						Math.min(value1.f2, value2.f2),
						Math.max(value1.f3, value2.f3));
				}
			})
			.map(
				new MapFunction <Tuple4 <Double, Double, Double, Double>, Double>() {
					@Override
					public Double map(Tuple4 <Double, Double, Double, Double> value) {
						return (value.f2 + value.f3) / 2.;
					}
				});
	}

	/**
	 * Convert `AccurateBinaryMetricsSummary` to `OutlierMetrics`, replace original LABEL_ARRAY and add
	 * OUTLIER_VALUE_ARRAY, and serialize to `Row`
	 *
	 * @param metricsSummary
	 * @param realLabelsDataSet
	 * @param outlierValues
	 * @param predBasedCM
	 * @return
	 */
	static DataSet <Row> metricsSummaryToMetrics(DataSet <BaseMetricsSummary> metricsSummary,
												 DataSet <String> realLabelsDataSet,
												 String[] outlierValues,
												 DataSet <long[][]> predBasedCM) {
		final String REAL_LABELS_BC_NAME = "real_labels";
		return metricsSummary
			.map(new RichMapFunction <BaseMetricsSummary, Row>() {
				List <String> realLabels;
				long[][] predBasedCM;

				@Override
				public void open(Configuration parameters) {
					realLabels = getRuntimeContext().getBroadcastVariable(REAL_LABELS_BC_NAME);
					realLabels.sort(Collections.reverseOrder());
					predBasedCM = getRuntimeContext(). <long[][]>getBroadcastVariable(PRED_BASED_CM).get(0);
				}

				@Override
				public Row map(BaseMetricsSummary value) {
					AccurateBinaryMetricsSummary summary = (AccurateBinaryMetricsSummary) value;
					BinaryClassMetrics binaryClassMetrics = summary.toMetrics();
					OutlierMetrics metrics = new OutlierMetrics(binaryClassMetrics.getParams());
					metrics.set(OutlierMetrics.OUTLIER_VALUE_ARRAY, outlierValues);
					// Set confusion matrix related metrics
					setMiddleThreParams(metrics.getParams(), new ConfusionMatrix(predBasedCM),
						realLabels.toArray(new String[0]));
					return metrics.serialize();
				}
			})
			.withBroadcastSet(realLabelsDataSet, REAL_LABELS_BC_NAME)
			.withBroadcastSet(predBasedCM, PRED_BASED_CM);
	}

	@Override
	public OutlierMetrics createMetrics(List <Row> rows) {
		return new OutlierMetrics(rows.get(0));
	}

	public EvalOutlierBatchOp lazyVizMetrics() {
		//noinspection unchecked
		return lazyCollectMetrics(d -> {
			OutlierMetricsHtmlVisualizer visualizer = OutlierMetricsHtmlVisualizer.getInstance();
			visualizer.visualize(d);
		});
	}

	@Override
	public EvalOutlierBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = get(EvalOutlierParams.LABEL_COL);
		String[] outlierValues = get(EvalOutlierParams.OUTLIER_VALUE_STRINGS);
		AkPreconditions.checkArgument(outlierValues.length > 0,
			new AkIllegalOperatorParameterException("Must provide at least 1 outlier values."));
		Set <String> outlierValueSet = new HashSet <>(Arrays.asList(outlierValues));

		AkPreconditions.checkArgument(getParams().contains(EvalOutlierParams.PREDICTION_DETAIL_COL),
			new AkIllegalOperatorParameterException("Outlier detection evaluation must give predictionDetailCol!"));

		String predDetailColName = get(EvalOutlierParams.PREDICTION_DETAIL_COL);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		DataSet <Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();
		data = data.flatMap(new FilterInvalid(0, 1));
		DataSet <String> realLabels = calcRealLabels(data);

		DataSet <Double> decisionThreshold = calcDecisionThreshold(data);

		data = data.map(new ReplaceLabelMapFunction(outlierValueSet, 1));
		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels =
			data.getExecutionEnvironment().fromElements(LABEL_INFO);

		DataSet <Tuple3 <Double, Boolean, Double>> sampleStatistics =
			calcSampleStatistics(data, labels, Types.STRING, decisionThreshold, new ProbMapExtractor());
		DataSet <BaseMetricsSummary> res = calLabelPredDetailLocal(labels, sampleStatistics, decisionThreshold);
		DataSet <BaseMetricsSummary> metricsSummary = res.reduce(new EvaluationUtil.ReduceBaseMetrics());

		DataSet <long[][]> predBasedCM = calcPredictionBasedConfusionMatrix(data, 0, 1, labels);
		DataSet <Row> metricsJson = metricsSummaryToMetrics(metricsSummary, realLabels, outlierValues, predBasedCM);
		setOutput(metricsJson, new String[] {"Data"}, new TypeInformation[] {Types.STRING});
		return this;
	}

	static class FilterInvalid implements FlatMapFunction <Row, Row> {
		int[] checkIndices;

		public FilterInvalid(int... checkIndices) {
			this.checkIndices = checkIndices;
		}

		@Override
		public void flatMap(Row value, Collector <Row> out) throws Exception {
			if (null == value) {
				return;
			}
			for (int i = 0; i < checkIndices.length; i += 1) {
				if (null == value.getField(i)) {
					return;
				}
			}
			out.collect(value);
		}
	}

	DataSet <long[][]> calcPredictionBasedConfusionMatrix(DataSet <Row> data, int actualLabelIdx, int predDetailIdx,
														  DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labelInfo) {
		DataSet <Tuple2 <Boolean, Boolean>> actualPredLabels = data.rebalance()
			.map(new CalcRealPredLabels(actualLabelIdx, predDetailIdx))
			.withBroadcastSet(labelInfo, LABELS_BC_NAME);
		DataSet <long[][]> partitionCMs = actualPredLabels.mapPartition(new CalcConfusionMatrix());
		return partitionCMs.reduceGroup(new AccConfusionMatrix());
	}

	static class CalcRealPredLabels extends RichMapFunction <Row, Tuple2 <Boolean, Boolean>> {
		private final int actualLabelIdx;
		private final int predDetailIdx;
		private Object[] labels;

		CalcRealPredLabels(int actualLabelIdx, int predDetailIdx) {
			this.actualLabelIdx = actualLabelIdx;
			this.predDetailIdx = predDetailIdx;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list =
				getRuntimeContext().getBroadcastVariable(LABELS_BC_NAME);
			AkPreconditions.checkState(list.size() > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
			labels = list.get(0).f1;
		}

		@Override
		public Tuple2 <Boolean, Boolean> map(Row value) throws Exception {
			boolean isActualPos = labels[0].equals(value.getField(actualLabelIdx));
			String predDetailStr = (String) value.getField(predDetailIdx);
			Map <String, Object> predDetailMap = JsonConverter.fromJson(predDetailStr,
				new TypeReference <HashMap <String, Object>>() {}.getType());
			boolean isPredPos = Boolean.parseBoolean(predDetailMap.get(OutlierDetector.IS_OUTLIER_KEY).toString());
			return Tuple2.of(isActualPos, isPredPos);
		}
	}

	static class CalcConfusionMatrix implements MapPartitionFunction <Tuple2 <Boolean, Boolean>, long[][]> {
		@Override
		public void mapPartition(Iterable <Tuple2 <Boolean, Boolean>> values, Collector <long[][]> out)
			throws Exception {
			long[][] cm = new long[2][2];
			for (Tuple2 <Boolean, Boolean> value : values) {
				int rowIndex = value.f1 ? 0 : 1;
				int colIndex = value.f0 ? 0 : 1;
				cm[rowIndex][colIndex] += 1;
			}
			out.collect(cm);
		}
	}

	static class AccConfusionMatrix implements GroupReduceFunction <long[][], long[][]> {
		@Override
		public void reduce(Iterable <long[][]> values, Collector <long[][]> out) throws Exception {
			long[][] cm = new long[2][2];
			for (long[][] value : values) {
				for (int i = 0; i < cm.length; i += 1) {
					for (int j = 0; j < cm[0].length; j += 1) {
						cm[i][j] += value[i][j];
					}
				}
			}
			out.collect(cm);
		}
	}
}
