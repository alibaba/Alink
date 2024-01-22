package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.AccurateBinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.ConfusionMatrix;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.ProbMapExtractor;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.operator.common.outlier.OutlierDetector;
import com.alibaba.alink.operator.common.utils.OutlierMetricsHtmlVisualizer;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalOutlierParams;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.operator.common.evaluation.BinaryMetricsSummary.setMiddleThreParams;
import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.calcSampleStatistics;
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
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionDetailCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("异常检测评估")
@NameEn("Evaluation of Outlier Detection")
public class EvalOutlierLocalOp extends LocalOperator <EvalOutlierLocalOp> implements
	EvalOutlierParams <EvalOutlierLocalOp>,
	EvaluationMetricsCollector <OutlierMetrics, EvalOutlierLocalOp> {

	private static final Logger LOG = LoggerFactory.getLogger(EvalOutlierLocalOp.class);

	public EvalOutlierLocalOp() {
		this(null);
	}

	public EvalOutlierLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = get(EvalOutlierParams.LABEL_COL);
		String[] outlierValues = get(EvalOutlierParams.OUTLIER_VALUE_STRINGS);
		AkPreconditions.checkArgument(outlierValues.length > 0,
			new AkIllegalOperatorParameterException("Must provide at least 1 outlier values."));
		Set <String> outlierValueSet = new HashSet <>(Arrays.asList(outlierValues));

		AkPreconditions.checkArgument(getParams().contains(EvalOutlierParams.PREDICTION_DETAIL_COL),
			new AkIllegalOperatorParameterException("Outlier detection evaluation must give "
				+ "predictionDetailCol!"));

		String predDetailColName = get(EvalOutlierParams.PREDICTION_DETAIL_COL);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		MTable mt = in.select(new String[] {labelColName, predDetailColName}).getOutputTable();
		ArrayList <Row> data = new ArrayList <>();
		HashSet <String> realLabels = new HashSet <>();
		for (Row row : mt.getRows()) {
			if (null != row) {
				String label = outlierValueSet.contains(String.valueOf(row.getField(0)))
					? OUTLIER_LABEL
					: INLIER_LABEL;
				data.add(Row.of(label, row.getField(1)));
				realLabels.add(label);
			}
		}

		double decisionThreshold = calcDecisionThreshold(data);

		Tuple2 <Map <Object, Integer>, Object[]> labels = EvalOutlierUtils.LABEL_INFO;

		List <Tuple3 <Double, Boolean, Double>> sampleStatistics =
			calcSampleStatistics(data, labels, Types.STRING, decisionThreshold, new ProbMapExtractor());
		AccurateBinaryMetricsSummary summary = ClassificationEvaluationUtil
			.calLabelPredDetailLocal(labels, sampleStatistics, decisionThreshold);

		long[][] predBasedCM = calcPredictionBasedConfusionMatrix(data, 0, 1, labels);

		BinaryClassMetrics binaryClassMetrics = summary.toMetrics();
		OutlierMetrics metrics = new OutlierMetrics(binaryClassMetrics.getParams());
		metrics.set(OutlierMetrics.OUTLIER_VALUE_ARRAY, outlierValues);
		setMiddleThreParams(metrics.getParams(), new ConfusionMatrix(predBasedCM), realLabels.toArray(new String[0]));

		setOutputTable(new MTable(new Row[] {metrics.serialize()}, new TableSchema(
			new String[] {"outlier_eval_result"}, new TypeInformation[] {Types.STRING})));
	}

	@Override
	public OutlierMetrics createMetrics(List <Row> rows) {
		return new OutlierMetrics(rows.get(0));
	}

	@Override
	public OutlierMetrics collectMetrics() {
		return EvaluationMetricsCollector.super.collectMetrics();
	}

	long[][] calcPredictionBasedConfusionMatrix(List <Row> data, int actualLabelIdx, int predDetailIdx,
												Tuple2 <Map <Object, Integer>, Object[]> labelInfo) {
		Object[] labels = labelInfo.f1;
		ArrayList <Tuple2 <Boolean, Boolean>> values = new ArrayList <>();
		for (Row value : data) {
			boolean isActualPos = labels[0].equals(value.getField(actualLabelIdx));
			String predDetailStr = (String) value.getField(predDetailIdx);
			Map <String, Object> predDetailMap = JsonConverter.fromJson(predDetailStr,
				new TypeReference <HashMap <String, Object>>() {}.getType());
			boolean isPredPos = Boolean.parseBoolean(predDetailMap.get(OutlierDetector.IS_OUTLIER_KEY).toString());
			values.add(Tuple2.of(isActualPos, isPredPos));
		}

		long[][] cm = new long[2][2];
		for (Tuple2 <Boolean, Boolean> value : values) {
			int rowIndex = value.f1 ? 0 : 1;
			int colIndex = value.f0 ? 0 : 1;
			cm[rowIndex][colIndex] += 1;
		}

		return cm;

	}

	private static double calcDecisionThreshold(List <Row> values) {
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

		return (scoreBoundary.f2 + scoreBoundary.f3) / 2.;

	}

	public EvalOutlierLocalOp lazyVizMetrics() {
		//noinspection unchecked
		return lazyCollectMetrics(d -> {
			OutlierMetricsHtmlVisualizer visualizer = OutlierMetricsHtmlVisualizer.getInstance();
			visualizer.visualize(d);
		});
	}

}
