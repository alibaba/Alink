package com.alibaba.alink.operator.batch.evaluation;

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
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.RankingMetrics;
import com.alibaba.alink.params.evaluation.EvalRankingParams;

import java.util.List;

/**
 * Evaluation for ranking system.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@NameCn("排序评估")
@NameEn("Eval Ranking")
public class EvalRankingBatchOp extends BatchOperator <EvalRankingBatchOp>
	implements EvalRankingParams <EvalRankingBatchOp>, EvaluationMetricsCollector <RankingMetrics,
	EvalRankingBatchOp> {

	private static final long serialVersionUID = 4418406919511122133L;

	public EvalRankingBatchOp() {
		super(null);
	}

	public EvalRankingBatchOp(Params params) {
		super(params);
	}

	@Override
	public EvalRankingBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> in = checkAndGetFirst(inputs);

		TableUtil.assertSelectedColExist(in.getColNames(), this.getLabelCol());
		TableUtil.assertSelectedColExist(in.getColNames(), this.getPredictionCol());

		DataSet <Row> dataSet = in.select(new String[] {this.getLabelCol(), this.getPredictionCol()}).getDataSet();
		DataSet <Tuple3 <Integer, Class, Integer>> labels = EvalMultiLabelBatchOp.getLabelNumberAndMaxK(
			dataSet, getLabelRankingInfo(), getPredictionRankingInfo()
		);
		DataSet <Row> out = dataSet
			.rebalance()
			.mapPartition(new CalcLocal(getLabelRankingInfo(), getPredictionRankingInfo()))
			.withBroadcastSet(labels, EvalMultiLabelBatchOp.LABELS)
			.reduce(new EvaluationUtil.ReduceBaseMetrics())
			.flatMap(new EvaluationUtil.SaveDataAsParams());

		this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
			out, new TableSchema(new String[] {"data"}, new TypeInformation[] {Types.STRING})
		));
		return this;
	}

	/**
	 * Get the MultiLabelMetrics.
	 */
	public static class CalcLocal extends RichMapPartitionFunction <Row, BaseMetricsSummary> {
		private static final long serialVersionUID = -2274636215166393789L;
		String labelKObject;
		String predictionKObject;

		public CalcLocal(String labelKObject, String predictionKObject) {
			this.labelKObject = labelKObject;
			this.predictionKObject = predictionKObject;
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <BaseMetricsSummary> collector) throws Exception {
			Tuple3 <Integer, Class, Integer> labelSizeClass =
				getRuntimeContext()
					. <Tuple3 <Integer, Class, Integer>>getBroadcastVariable(EvalMultiLabelBatchOp.LABELS)
					.get(0);
			collector.collect(
				EvaluationUtil.getRankingMetrics(
					rows, labelSizeClass, labelKObject, predictionKObject
				)
			);
		}
	}

	@Override
	public RankingMetrics createMetrics(List <Row> rows) {
		return new RankingMetrics(rows.get(0));
	}
}
