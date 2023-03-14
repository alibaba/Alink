package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
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
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getMultiClassMetrics;

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
@NameEn("Eval Multi Class")
public class EvalMultiClassBatchOp extends BatchOperator <EvalMultiClassBatchOp>
	implements EvalMultiClassParams <EvalMultiClassBatchOp>,
	EvaluationMetricsCollector <MultiClassMetrics, EvalMultiClassBatchOp> {

	private static final String LABELS = "labels";
	private static final String DATA_OUTPUT = "Data";
	private static final long serialVersionUID = -2027803227905959081L;

	public EvalMultiClassBatchOp() {
		this(null);
	}

	public EvalMultiClassBatchOp(Params params) {
		super(params);
	}

	@Override
	public MultiClassMetrics createMetrics(List <Row> rows) {
		return new MultiClassMetrics(rows.get(0));
	}

	@Override
	public EvalMultiClassBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = this.get(EvalMultiClassParams.LABEL_COL);
		TypeInformation labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = this.get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

		// Judge the evaluation type from params.
		ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(this.getParams());
		DataSet <BaseMetricsSummary> res;
		switch (type) {
			case PRED_RESULT: {
				String predResultColName = this.get(EvalMultiClassParams.PREDICTION_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predResultColName);

				DataSet <Row> data = in.select(new String[] {labelColName, predResultColName}).getDataSet();
				res = calLabelPredictionLocal(data, positiveValue, labelType);
				break;
			}
			case PRED_DETAIL: {
				String predDetailColName = this.get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
				TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

				DataSet <Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();
				res = calLabelPredDetailLocal(data, positiveValue, labelType);
				break;
			}
			default: {
				throw new AkUnsupportedOperationException("Unsupported evaluation type: " + type);
			}
		}

		DataSet <BaseMetricsSummary> metrics = res
			.reduce(new EvaluationUtil.ReduceBaseMetrics());

		this.setOutput(metrics.flatMap(new EvaluationUtil.SaveDataAsParams()),
			new String[] {DATA_OUTPUT}, new TypeInformation[] {Types.STRING});

		return this;
	}

	/**
	 * Calculate the evaluation metrics of every partition in case of inputs are prediction result.
	 */
	private static DataSet <BaseMetricsSummary> calLabelPredictionLocal(DataSet <Row> data,
																		final String positiveValue,
																		TypeInformation labelType) {

		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels = data.flatMap(new FlatMapFunction <Row, Object>() {
			private static final long serialVersionUID = -120689740292597906L;

			@Override
			public void flatMap(Row row, Collector <Object> collector) {
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					collector.collect(row.getField(0));
					collector.collect(row.getField(1));
				}
			}
		}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, positiveValue, labelType, false));

		// Build the confusion matrix.
		return data
			.rebalance()
			.mapPartition(new CalLabelPredictionLocal())
			.withBroadcastSet(labels, LABELS);
	}

	/**
	 * Calculate the evaluation metrics of every partition in case of inputs are label and prediction detail.
	 */
	private static DataSet <BaseMetricsSummary> calLabelPredDetailLocal(DataSet <Row> data,
																		final String positiveValue,
																		TypeInformation labelType) {
		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels = data.flatMap(new FlatMapFunction <Row, Object>() {
			private static final long serialVersionUID = 7858786264569432008L;

			@Override
			public void flatMap(Row row, Collector <Object> collector) {
				TreeMap <Object, Double> labelProbMap;
				if (EvaluationUtil.checkRowFieldNotNull(row)) {
					labelProbMap = EvaluationUtil.extractLabelProbMap(row, labelType);
					labelProbMap.keySet().forEach(collector::collect);
					collector.collect(row.getField(0));
				}
			}
		}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(false, positiveValue, labelType, false));

		return data
			.rebalance()
			.mapPartition(new CalLabelDetailLocal(labelType))
			.withBroadcastSet(labels, LABELS);
	}

	/**
	 * Calculate the confusion matrix based on the label and predResult.
	 */
	static class CalLabelPredictionLocal extends RichMapPartitionFunction <Row, BaseMetricsSummary> {
		private static final long serialVersionUID = -2439136352527525005L;
		private Tuple2 <Map <Object, Integer>, Object[]> map;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			AkPreconditions.checkState(list.size() > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
			this.map = list.get(0);
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <BaseMetricsSummary> collector) {
			collector.collect(getMultiClassMetrics(rows, map));
		}
	}

	/**
	 * Calculate the confusion matrix based on the label and predResult.
	 */
	static class CalLabelDetailLocal extends RichMapPartitionFunction <Row, BaseMetricsSummary> {
		private static final long serialVersionUID = 5680342197308160013L;
		private Tuple2 <Map <Object, Integer>, Object[]> map;
		private TypeInformation labelType;

		public CalLabelDetailLocal(TypeInformation labelType) {
			this.labelType = labelType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			AkPreconditions.checkState(list.size() > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
			this.map = list.get(0);
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <BaseMetricsSummary> collector) {
			collector.collect(getDetailStatistics(rows, false, map, labelType));
		}
	}
}
