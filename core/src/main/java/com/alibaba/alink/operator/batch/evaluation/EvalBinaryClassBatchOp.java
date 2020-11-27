package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.AccurateBinaryMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp.LABELS;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.checkRowFieldNotNull;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.extractLabelProbMap;

/**
 * Calculate the evaluation metrics for binary classifiction.
 *
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 *
 * PositiveValue is optional, if given, it will be placed at the first position in the output label Array.
 * If not given, the labels are sorted in descending order.
 */
public class EvalBinaryClassBatchOp extends BatchOperator <EvalBinaryClassBatchOp> implements
	EvalBinaryClassParams <EvalBinaryClassBatchOp>,
	EvaluationMetricsCollector <BinaryClassMetrics, EvalBinaryClassBatchOp> {

	private static final long serialVersionUID = 5413408734356661786L;
	private static final Logger LOG = LoggerFactory.getLogger(EvalBinaryClassBatchOp.class);

	public EvalBinaryClassBatchOp() {
		this(null);
	}

	public EvalBinaryClassBatchOp(Params params) {
		super(params);
	}

	@Override
	public BinaryClassMetrics createMetrics(List <Row> rows) {
		return new BinaryClassMetrics(rows.get(0));
	}

	@Override
	public EvalBinaryClassBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);
		String labelColName = this.get(EvalMultiClassParams.LABEL_COL);
		TypeInformation <?> labelType = TableUtil.findColTypeWithAssertAndHint(in.getSchema(), labelColName);
		String positiveValue = this.get(EvalBinaryClassParams.POS_LABEL_VAL_STR);

		Preconditions.checkArgument(getParams().contains(EvalBinaryClassParams.PREDICTION_DETAIL_COL),
			"Binary Evaluation must give predictionDetailCol!");

		String predDetailColName = this.get(EvalMultiClassParams.PREDICTION_DETAIL_COL);
		TableUtil.assertSelectedColExist(in.getColNames(), labelColName, predDetailColName);

		DataSet <Row> data = in.select(new String[] {labelColName, predDetailColName}).getDataSet();
		DataSet <BaseMetricsSummary> res = calLabelPredDetailLocal(data, positiveValue, labelType);
		DataSet <BaseMetricsSummary> metrics = res
			.reduce(new EvaluationUtil.ReduceBaseMetrics());

		this.setOutput(metrics.flatMap(new EvaluationUtil.SaveDataAsParams()),
			new String[] {"Data"}, new TypeInformation[] {Types.STRING});

		return this;
	}

	/**
	 * Calculate the evaluation metrics of every partition in case of inputs are label and prediction detail.
	 */
	private static DataSet <BaseMetricsSummary> calLabelPredDetailLocal(DataSet <Row> data,
																		final String positiveValue,
																		TypeInformation <?> labelType) {
		DataSet <Tuple2 <Map <Object, Integer>, Object[]>> labels = data.flatMap(new FlatMapFunction <Row, Object>() {
			private static final long serialVersionUID = 7858786264569432008L;

			@Override
			public void flatMap(Row row, Collector <Object> collector) {
				TreeMap <Object, Double> labelProbMap;
				if (checkRowFieldNotNull(row)) {
					labelProbMap = extractLabelProbMap(row, labelType);
					labelProbMap.keySet().forEach(collector::collect);
					collector.collect(row.getField(0));
				}
			}
		}).reduceGroup(new EvaluationUtil.DistinctLabelIndexMap(true, positiveValue, labelType, true));

		DataSet <Tuple3 <Double, Boolean, Double>> statistics = data
			.rebalance()
			.mapPartition(new CalLabelDetailLocal(labelType))
			.withBroadcastSet(labels, LABELS)
			.partitionByRange(0)
			.sortPartition(0, Order.DESCENDING);

		DataSet <ClassificationEvaluationUtil.BinaryPartitionSummary> partitionStatistics = statistics.mapPartition(
			new RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, ClassificationEvaluationUtil
				.BinaryPartitionSummary>() {
				private static final long serialVersionUID = 9012670438603117070L;

				@Override
				public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> values,
										 Collector <ClassificationEvaluationUtil.BinaryPartitionSummary> out)
					throws Exception {
					ClassificationEvaluationUtil.BinaryPartitionSummary statistics =
						new ClassificationEvaluationUtil.BinaryPartitionSummary(
							getRuntimeContext().getIndexOfThisSubtask(), 0, 0, 0
						);
					values.forEach(t -> ClassificationEvaluationUtil.updateBinaryPartitionSummary(statistics, t));
					out.collect(statistics);
				}
			});

		DataSet <Tuple1 <Double>> auc = statistics
			.mapPartition(new CalcAuc())
			.withBroadcastSet(partitionStatistics, "Statistics")
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction <Tuple3 <Double, Long, Boolean>, Tuple1 <Double>>() {
				private static final long serialVersionUID = -7442946470184046220L;

				@Override
				public void reduce(Iterable <Tuple3 <Double, Long, Boolean>> values, Collector <Tuple1 <Double>> out)
					throws Exception {
					long sum = 0;
					long cnt = 0;
					long positiveCnt = 0;
					for (Tuple3 <Double, Long, Boolean> t : values) {
						sum += t.f1;
						cnt++;
						if (t.f2) {
							positiveCnt++;
						}
					}
					out.collect(Tuple1.of(1. * sum / cnt * positiveCnt));
				}
			}).sum(0);

		return statistics.mapPartition(new CalcBinaryMetricsSummary())
			.withBroadcastSet(partitionStatistics, "Statistics")
			.withBroadcastSet(labels, LABELS)
			.withBroadcastSet(auc, "auc");
	}

	static class CalcAuc
		extends RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, Tuple3 <Double, Long, Boolean>> {
		private static final long serialVersionUID = 3047511137846831576L;
		private long startIndex;
		private long total;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <ClassificationEvaluationUtil.BinaryPartitionSummary> statistics = getRuntimeContext()
				.getBroadcastVariable("Statistics");
			Tuple2 <Boolean, long[]> t = ClassificationEvaluationUtil.reduceBinaryPartitionSummary(statistics,
				getRuntimeContext().getIndexOfThisSubtask());
			startIndex = t.f1[ClassificationEvaluationUtil.CUR_FALSE] + t.f1[ClassificationEvaluationUtil.CUR_TRUE]
				+ 1;
			total = t.f1[ClassificationEvaluationUtil.TOTAL_TRUE] + t.f1[ClassificationEvaluationUtil.TOTAL_FALSE];
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> values,
								 Collector <Tuple3 <Double, Long, Boolean>> out) throws Exception {
			for (Tuple3 <Double, Boolean, Double> t : values) {
				if (!ClassificationEvaluationUtil.isMiddlePoint(t)) {
					out.collect(Tuple3.of(t.f0, total - startIndex + 1, t.f1));
					startIndex++;
				}
			}
		}
	}

	static class CalcBinaryMetricsSummary
		extends RichMapPartitionFunction <Tuple3 <Double, Boolean, Double>, BaseMetricsSummary> {
		private static final long serialVersionUID = 5680342197308160013L;
		private Object[] labels;
		private long[] countValues;
		private boolean firstBin;
		private double auc;

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			Preconditions.checkArgument(list.size() > 0,
				"Please check the evaluation input! there is no effective row!");
			this.labels = list.get(0).f1;

			List <ClassificationEvaluationUtil.BinaryPartitionSummary> statistics = getRuntimeContext()
				.getBroadcastVariable("Statistics");
			Tuple2 <Boolean, long[]> t = ClassificationEvaluationUtil.reduceBinaryPartitionSummary(statistics,
				getRuntimeContext().getIndexOfThisSubtask());
			firstBin = t.f0;
			countValues = t.f1;

			auc = ((Tuple1 <Double>) getRuntimeContext().getBroadcastVariable("auc").get(0)).f0;
			long totalTrue = countValues[ClassificationEvaluationUtil.TOTAL_TRUE];
			long totalFalse = countValues[ClassificationEvaluationUtil.TOTAL_FALSE];
			if(totalTrue == 0){
				LOG.warn("There is no positive sample in data!");
			}
			if(totalFalse == 0){
				LOG.warn("There is no negativve sample in data!");
			}
			if(totalTrue > 0 && totalFalse > 0){
				auc = (auc - totalTrue * (totalTrue + 1) / 2) / (totalTrue * totalFalse);
			}else{
				auc = Double.NaN;
			}
		}

		@Override
		public void mapPartition(Iterable <Tuple3 <Double, Boolean, Double>> iterable,
								 Collector <BaseMetricsSummary> collector) {
			AccurateBinaryMetricsSummary summary = new AccurateBinaryMetricsSummary(labels, 0.0, 0L, auc);
			double[] tprFprPrecision = new double[ClassificationEvaluationUtil.RECORD_LEN];
			for (Tuple3 <Double, Boolean, Double> t : iterable) {
				ClassificationEvaluationUtil.updateAccurateBinaryMetricsSummary(
					t,
					summary,
					countValues,
					tprFprPrecision,
					firstBin);
			}
			collector.collect(summary);
		}
	}

	/**
	 * Calculate the confusion matrix based on the label and predResult.
	 */
	static class CalLabelDetailLocal extends RichMapPartitionFunction <Row, Tuple3 <Double, Boolean, Double>> {
		private static final long serialVersionUID = 5680342197308160013L;
		private Tuple2 <Map <Object, Integer>, Object[]> map;
		private TypeInformation <?> labelType;

		public CalLabelDetailLocal(TypeInformation <?> labelType) {
			this.labelType = labelType;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			List <Tuple2 <Map <Object, Integer>, Object[]>> list = getRuntimeContext().getBroadcastVariable(LABELS);
			Preconditions.checkArgument(list.size() > 0,
				"Please check the evaluation input! there is no effective row!");
			this.map = list.get(0);
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <Tuple3 <Double, Boolean, Double>> collector) {
			for (Row row : rows) {
				Tuple3 <Double, Boolean, Double> t = ClassificationEvaluationUtil.getBinaryDetailStatistics(row,
					this.map.f1, labelType);
				if (null != t) {
					collector.collect(t);
				}
			}
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				collector.collect(ClassificationEvaluationUtil.middlePoint);
			}
		}
	}

}
