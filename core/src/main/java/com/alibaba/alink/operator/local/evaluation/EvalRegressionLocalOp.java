package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.evaluation.EvalRegressionParams;

import java.util.List;

/**
 * Calculate the evaluation data for regression. The evaluation metrics are: SST: Sum of Squared for Total SSE: Sum of
 * Squares for Error SSR: Sum of Squares for Regression R^2: Coefficient of Determination R: Multiple CorrelationBak
 * Coeffient MSE: Mean Squared Error RMSE: Root Mean Squared Error SAE/SAD: Sum of Absolute Error/Difference MAE/MAD:
 * Mean Absolute Error/Difference MAPE: Mean Absolute Percentage Error
 */
@InputPorts(values = @PortSpec(PortType.DATA))
@OutputPorts(values = @PortSpec(PortType.EVAL_METRICS))
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionCol")
@NameCn("回归评估")
public final class EvalRegressionLocalOp extends LocalOperator <EvalRegressionLocalOp>
	implements EvalRegressionParams <EvalRegressionLocalOp>,
	EvaluationMetricsCollector <RegressionMetrics, EvalRegressionLocalOp> {

	private RegressionMetrics metrics;

	public EvalRegressionLocalOp() {
		super(null);
	}

	public EvalRegressionLocalOp(Params params) {
		super(params);
	}

	@Override
	public EvalRegressionLocalOp linkFrom(LocalOperator <?>... inputs) {
		LocalOperator in = checkAndGetFirst(inputs);

		TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getLabelCol());
		TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getPredictionCol());

		TableUtil.assertNumericalCols(in.getSchema(), this.getLabelCol(), this.getPredictionCol());

		this.metrics = EvaluationUtil.getRegressionStatistics(
			in.getOutputTable().select(this.getLabelCol(), this.getPredictionCol()).getRows()
		).toMetrics();

		this.setOutputTable(new MTable(
			new Row[] {metrics.serialize()},
			new TableSchema(new String[] {"regression_eval_result"}, new TypeInformation[] {Types.STRING})
		));
		return this;
	}

	@Override
	public RegressionMetrics createMetrics(List <Row> rows) {
		return new RegressionMetrics(rows.get(0));
	}

	@Override
	public RegressionMetrics collectMetrics() {
		return metrics;
	}

}
