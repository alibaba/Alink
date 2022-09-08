package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.RankingMetrics;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalRankingParams;

import java.util.List;

import static com.alibaba.alink.operator.local.evaluation.EvalMultiLabelLocalOp.getLabelNumberAndMaxK;

/**
 * Evaluation for ranking system.
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@NameCn("排序评估")
public class EvalRankingLocalOp extends LocalOperator <EvalRankingLocalOp>
	implements EvalRankingParams <EvalRankingLocalOp>,
	EvaluationMetricsCollector <RankingMetrics, EvalRankingLocalOp> {

	private RankingMetrics metrics;

	public EvalRankingLocalOp() {
		super(null);
	}

	public EvalRankingLocalOp(Params params) {
		super(params);
	}

	@Override
	public EvalRankingLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator <?> in = checkAndGetFirst(inputs);

		TableUtil.assertSelectedColExist(in.getColNames(), this.getLabelCol());
		TableUtil.assertSelectedColExist(in.getColNames(), this.getPredictionCol());

		List <Row> dataSet
			= in.select(new String[] {this.getLabelCol(), this.getPredictionCol()}).getOutputTable().getRows();

		Tuple3 <Integer, Class, Integer> labelSizeClass
			= getLabelNumberAndMaxK(dataSet, getPredictionRankingInfo(), getPredictionRankingInfo());

		this.metrics = EvaluationUtil.getRankingMetrics(
			in.getOutputTable().select(this.getLabelCol(), this.getPredictionCol()).getRows(),
			labelSizeClass, getLabelRankingInfo(), getPredictionRankingInfo()
		).toMetrics();

		this.setOutputTable(new MTable(
			new Row[] {metrics.serialize()},
			new TableSchema(new String[] {"ranking_eval_result"}, new TypeInformation[] {Types.STRING})
		));
		return this;
	}

	@Override
	public RankingMetrics createMetrics(List <Row> rows) {
		return new RankingMetrics(rows.get(0));
	}

	@Override
	public RankingMetrics collectMetrics() {
		return metrics;
	}

}
