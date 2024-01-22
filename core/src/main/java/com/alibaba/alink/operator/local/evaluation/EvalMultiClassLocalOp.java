package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.MultiMetricsSummary;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;

/**
 * Multi classification evaluation.
 * <p>
 * Calculate the evaluation metrics for multi classification.
 * <p>
 * You can either give label column and predResult column or give label column and predDetail column. Once predDetail
 * column is given, the predResult column is ignored.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionCol")
@ParamSelectColumnSpec(name = "predictionDetailCol", allowedTypeCollections = TypeCollections.STRING_TYPE)
@NameCn("多分类评估")
public class EvalMultiClassLocalOp extends LocalOperator <EvalMultiClassLocalOp>
	implements EvalMultiClassParams <EvalMultiClassLocalOp>,
	EvaluationMetricsCollector <MultiClassMetrics, EvalMultiClassLocalOp> {

	private static final String DATA_OUTPUT = "Data";

	public EvalMultiClassLocalOp() {
		this(null);
	}

	public EvalMultiClassLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = this.get(EvalMultiClassParams.LABEL_COL);
		TypeInformation labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = this.get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

		// Judge the evaluation type from params.
		ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());
		List <Row> data = null;
		DataSet <BaseMetricsSummary> res;
		MultiClassMetrics metrics;
		switch (type) {
			case PRED_RESULT: {
				String predResultColName = this.get(EvalMultiClassParams.PREDICTION_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predResultColName);

				data = in.select(new String[] {labelColName, predResultColName}).getOutputTable().getRows();
				Tuple2 <Map <Object, Integer>, Object[]> labels = calcLabels(data, false, positiveValue, labelType, type);
				MultiMetricsSummary summary =  EvaluationUtil.getMultiClassMetrics(data, labels);
				metrics = summary.toMetrics();
				break;
			}
			case PRED_DETAIL: {
				String predDetailColName = this.get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

				data = in.select(new String[] {labelColName, predDetailColName}).getOutputTable().getRows();
				Tuple2 <Map <Object, Integer>, Object[]> labels = calcLabels(data, false, positiveValue, labelType, type);
				MultiMetricsSummary summary = (MultiMetricsSummary) getDetailStatistics(data, false, labels, labelType);
				metrics = summary.toMetrics();
				break;
			}
			default: {
				throw new AkUnsupportedOperationException("Unsupported evaluation type: " + type);
			}
		}

		setOutputTable(new MTable(new Row[] {metrics.serialize()}, new TableSchema(
			new String[] {DATA_OUTPUT}, new TypeInformation[] {Types.STRING})));
	}

	private static Tuple2 <Map <Object, Integer>, Object[]> calcLabels(
		List <Row> data, boolean binary, final String positiveValue, TypeInformation <?> labelType,
		ClassificationEvaluationUtil.Type type
	) {
		switch (type) {
			case PRED_RESULT: {
				HashSet <Object> labels = new HashSet <>();
				for (Row row : data) {
					if (null != row.getField(0)) {
						labels.add(row.getField(0));
					}
					if (null != row.getField(1)) {
						labels.add(row.getField(1));
					}
				}
				return ClassificationEvaluationUtil.buildLabelIndexLabelArray(
					labels, binary, positiveValue, labelType, true);
			}
			case PRED_DETAIL: {
				Set <Object> labels = EvaluationUtil.extractLabelProbMap(data.get(0), labelType).keySet();
				return ClassificationEvaluationUtil.buildLabelIndexLabelArray(
					labels, binary, positiveValue, labelType, true);
			}
			default: {
				throw new AkUnsupportedOperationException("Unsupported evaluation type: " + type);
			}
		}
	}

	@Override
	public MultiClassMetrics createMetrics(List <Row> rows) {
		return new MultiClassMetrics(rows.get(0));
	}

	@Override
	public MultiClassMetrics collectMetrics() {
		return EvaluationMetricsCollector.super.collectMetrics();
	}
}
