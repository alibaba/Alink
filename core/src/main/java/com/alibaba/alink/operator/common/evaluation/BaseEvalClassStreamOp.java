package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.utils.TimeUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil.prependTagMapFunction;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalBinaryClassStreamParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassStreamParams;

import java.util.HashSet;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.buildLabelIndexLabelArray;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getMultiClassMetrics;

/**
 * Base class for EvalBinaryClassStreamOp and EvalMultiClassStreamOp. Calculate the evaluation data within time windows
 * for binary classification and multi classification. You can either give label column and predResult column or give
 * label column and predDetail column. Once predDetail column is given, the predResult column is ignored.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@Internal
public class BaseEvalClassStreamOp<T extends BaseEvalClassStreamOp <T>> extends StreamOperator <T> {
	private static final String DATA_OUTPUT = "Data";

	private static final long serialVersionUID = -6277527784116345678L;
	private final boolean binary;

	public BaseEvalClassStreamOp(Params params, boolean binary) {
		super(params);
		this.binary = binary;
	}

	@Override
	public T linkFrom(StreamOperator <?>... inputs) {
		StreamOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = this.get(EvalMultiClassStreamParams.LABEL_COL);
		TypeInformation<?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = this.get(EvalBinaryClassStreamParams.POS_LABEL_VAL_STR);
		double timeInterval = this.get(EvalMultiClassStreamParams.TIME_INTERVAL);

		if (binary) {
			if (!getParams().contains(EvalBinaryClassParams.PREDICTION_DETAIL_COL)) {
				throw new AkIllegalOperatorParameterException("Binary Evaluation must give predictionDetailCol!");
			}
		}

		ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());

		DataStream <BaseMetricsSummary> statistics;

		switch (type) {
			case PRED_RESULT: {
				String predResultColName = this.get(EvalMultiClassStreamParams.PREDICTION_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predResultColName);

				LabelPredictionWindow predMultiWindowFunction = new LabelPredictionWindow(binary, positiveValue,
					labelType);
				statistics = in.select(new String[] {labelColName, predResultColName})
					.getDataStream()
					.timeWindowAll(TimeUtil.convertTime(timeInterval))
					.apply(predMultiWindowFunction);
				break;
			}
			case PRED_DETAIL: {
				String predDetailColName = this.get(EvalMultiClassStreamParams.PREDICTION_DETAIL_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

				PredDetailLabel eval = new PredDetailLabel(positiveValue, binary, labelType);

				statistics = in.select(new String[] {labelColName, predDetailColName})
					.getDataStream()
					.timeWindowAll(TimeUtil.convertTime(timeInterval))
					.apply(eval);
				break;
			}
			default: {
				throw new AkUnsupportedOperationException("Unsupported evaluation type: " + type);
			}
		}
		DataStream <BaseMetricsSummary> totalStatistics = statistics
			.map(new EvaluationUtil.AllDataMerge())
			.setParallelism(1);

		DataStream <Row> windowOutput = statistics.map(
			new prependTagMapFunction(ClassificationEvaluationUtil.WINDOW.f0));
		DataStream <Row> allOutput = totalStatistics.map(
			new prependTagMapFunction(ClassificationEvaluationUtil.ALL.f0));

		DataStream <Row> union = windowOutput.union(allOutput);

		this.setOutput(union,
			new String[] {ClassificationEvaluationUtil.STATISTICS_OUTPUT, DATA_OUTPUT},
			new TypeInformation[] {Types.STRING, Types.STRING});

		return (T) this;
	}

	static class LabelPredictionWindow implements AllWindowFunction <Row, BaseMetricsSummary, TimeWindow> {
		private static final long serialVersionUID = -4426213828656690161L;
		private final boolean binary;
		private final String positiveValue;
		private final TypeInformation labelType;

		LabelPredictionWindow(boolean binary, String positiveValue, TypeInformation labelType) {
			this.binary = binary;
			this.positiveValue = positiveValue;
			this.labelType = labelType;
		}

		@Override
		public void apply(TimeWindow timeWindow, Iterable <Row> rows, Collector <BaseMetricsSummary> collector)
			throws Exception {
			HashSet <Object> labels = new HashSet <>();
			for (Row row : rows) {
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					labels.add(row.getField(0));
					labels.add(row.getField(1));
				}
			}
			if (labels.size() > 0) {
				collector.collect(getMultiClassMetrics(rows,
					buildLabelIndexLabelArray(labels, binary, positiveValue, labelType, true)));
			}
		}
	}

	static class PredDetailLabel implements AllWindowFunction <Row, BaseMetricsSummary, TimeWindow> {
		private static final long serialVersionUID = 8057305974098408321L;
		private final String positiveValue;
		private final Boolean binary;
		private final TypeInformation labelType;

		PredDetailLabel(String positiveValue, boolean binary, TypeInformation labelType) {
			this.positiveValue = positiveValue;
			this.binary = binary;
			this.labelType = labelType;
		}

		@Override
		public void apply(TimeWindow timeWindow, Iterable <Row> rows, Collector <BaseMetricsSummary> collector)
			throws Exception {
			HashSet <Object> labels = new HashSet <>();
			for (Row row : rows) {
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					labels.addAll(EvaluationUtil.extractLabelProbMap(row, labelType).keySet());
					labels.add(row.getField(0));
				}
			}
			if (labels.size() > 0) {
				collector.collect(
					getDetailStatistics(rows, binary,
						buildLabelIndexLabelArray(labels, binary, positiveValue, labelType, true), labelType));
			}
		}
	}

}
