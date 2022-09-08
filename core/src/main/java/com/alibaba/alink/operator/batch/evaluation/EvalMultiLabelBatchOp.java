package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil.ReduceBaseMetrics;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil.SaveDataAsParams;
import com.alibaba.alink.operator.common.evaluation.MultiLabelMetrics;
import com.alibaba.alink.operator.local.evaluation.EvalMultiLabelLocalOp;
import com.alibaba.alink.params.evaluation.EvalMultiLabelParams;

import java.util.HashSet;
import java.util.List;

/**
 * Evaluation for multi-label classification task.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@NameCn("多标签分类评估")
public class EvalMultiLabelBatchOp extends BatchOperator <EvalMultiLabelBatchOp>
	implements EvalMultiLabelParams <EvalMultiLabelBatchOp>,
	EvaluationMetricsCollector <MultiLabelMetrics, EvalMultiLabelBatchOp> {
	private static final long serialVersionUID = -1588545393316444529L;
	public static String LABELS = "labels";

	public EvalMultiLabelBatchOp() {
		super(null);
	}

	public EvalMultiLabelBatchOp(Params params) {
		super(params);
	}

	@Override
	public EvalMultiLabelBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);

		int indexLabel = TableUtil.findColIndex(in.getColNames(), this.getLabelCol());
		int indexPredict = TableUtil.findColIndex(in.getColNames(), this.getPredictionCol());
		AkPreconditions.checkArgument(indexLabel >= 0 && indexPredict >= 0,
			new AkIllegalOperatorParameterException("Can not find label column or prediction column!"));

		DataSet <Row> dataSet = in.select(new String[] {this.getLabelCol(), this.getPredictionCol()}).getDataSet();
		DataSet <Tuple3 <Integer, Class, Integer>> labels = getLabelNumberAndMaxK(dataSet, getPredictionRankingInfo(),
			getPredictionRankingInfo());
		DataSet <Row> out = dataSet
			.rebalance()
			.mapPartition(new CalcLocal(getLabelRankingInfo(), getPredictionRankingInfo()))
			.withBroadcastSet(labels, LABELS)
			.reduce(new ReduceBaseMetrics())
			.flatMap(new SaveDataAsParams());

		this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
			out, new TableSchema(new String[] {"data"}, new TypeInformation[] {Types.STRING})
		));
		return this;
	}

	/**
	 * Extract the label number and maxK.
	 *
	 * @param data              dataset
	 * @param labelKObject      the key for the label json
	 * @param predictionKObject the key for the prediction json
	 * @return LabelNumber, Label class, MaxK
	 */
	public static DataSet <Tuple3 <Integer, Class, Integer>> getLabelNumberAndMaxK(
		DataSet <Row> data, String labelKObject, String predictionKObject) {
		return data
			.map(new MapFunction <Row, Tuple3 <HashSet <Object>, Class, Integer>>() {
				private static final long serialVersionUID = -8707995574529447106L;

				@Override
				public Tuple3 <HashSet <Object>, Class, Integer> map(Row value) {
					return EvalMultiLabelLocalOp.subGetLabelNumberAndMaxK(value, labelKObject, predictionKObject);
				}
			}).reduce(new ReduceFunction <Tuple3 <HashSet <Object>, Class, Integer>>() {
				private static final long serialVersionUID = -2831334156409607751L;

				@Override
				public Tuple3 <HashSet <Object>, Class, Integer> reduce(
					Tuple3 <HashSet <Object>, Class, Integer> value1, Tuple3 <HashSet <Object>, Class, Integer> value2) {
					if (null == value1) {
						return value2;
					} else if (null == value2) {
						return value1;
					} else {
						if (value1.f1 == null) {
							AkPreconditions.checkArgument(value1.f0.size() == 0 && value1.f2 == 0,
								"LabelClass is null but label size is not 0!");
							return value2;
						} else if (value2.f1 == null) {
							AkPreconditions.checkArgument(value2.f0.size() == 0 && value2.f2 == 0,
								"LabelClass is null but label size is not 0!");
							return value1;
						} else if (value1.f1.equals(value2.f1)) {
							value1.f0.addAll(value2.f0);
							value1.f2 = Math.max(value1.f2, value2.f2);
							return value1;
						} else {
							HashSet <Object> hashSet = new HashSet <>();
							for (Object object : value1.f0) {
								hashSet.add(object.toString());
							}
							for (Object object : value2.f0) {
								hashSet.add(object.toString());
							}
							return Tuple3.of(hashSet, String.class, Math.max(value1.f2, value2.f2));
						}
					}
				}
			}).map(new MapFunction <Tuple3 <HashSet <Object>, Class, Integer>, Tuple3 <Integer, Class, Integer>>() {
				private static final long serialVersionUID = 3235026163541463499L;

				@Override
				public Tuple3 <Integer, Class, Integer> map(Tuple3 <HashSet <Object>, Class, Integer> value) {
					AkPreconditions.checkState(value.f0.size() > 0,
						"There is no valid data in the whole dataSet, please check the input for evaluation!");
					return Tuple3.of(value.f0.size(), value.f1, value.f2);
				}
			});
	}

	/**
	 * Get the MultiLabelMetrics.
	 */
	public static class CalcLocal extends RichMapPartitionFunction <Row, BaseMetricsSummary> {
		private static final long serialVersionUID = -9061749725428161379L;
		String labelKObject;
		String predictionKObject;

		public CalcLocal(String labelKObject, String predictionKObject) {
			this.labelKObject = labelKObject;
			this.predictionKObject = predictionKObject;
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <BaseMetricsSummary> collector) throws Exception {
			Tuple3 <Integer, Class, Integer> labelSizeClass = (Tuple3 <Integer, Class, Integer>) getRuntimeContext()
				.getBroadcastVariable(LABELS).get(0);
			collector.collect(
				EvaluationUtil.getMultiLabelMetrics(rows, labelSizeClass, labelKObject, predictionKObject));
		}
	}

	@Override
	public MultiLabelMetrics createMetrics(List <Row> rows) {
		return new MultiLabelMetrics(rows.get(0));
	}
}
