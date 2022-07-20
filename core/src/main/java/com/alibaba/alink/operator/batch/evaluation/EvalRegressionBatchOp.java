package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.annotation.InputPorts;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.OutputPorts;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.PortSpec;
import com.alibaba.alink.common.annotation.PortType;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataSetUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.BaseMetricsSummary;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.EvaluationUtil;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.params.evaluation.EvalRegressionParams;

import java.util.List;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getRegressionStatistics;

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
public final class EvalRegressionBatchOp extends BatchOperator <EvalRegressionBatchOp>
	implements EvalRegressionParams <EvalRegressionBatchOp>,
	EvaluationMetricsCollector <RegressionMetrics, EvalRegressionBatchOp> {

	private static final long serialVersionUID = -1780926771177908475L;

	public EvalRegressionBatchOp() {
		super(null);
	}

	public EvalRegressionBatchOp(Params params) {
		super(params);
	}

	@Override
	public EvalRegressionBatchOp linkFrom(BatchOperator <?>... inputs) {
		BatchOperator in = checkAndGetFirst(inputs);

		TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getLabelCol());
		TableUtil.findColIndexWithAssertAndHint(in.getColNames(), this.getPredictionCol());

		TableUtil.assertNumericalCols(in.getSchema(), this.getLabelCol(), this.getPredictionCol());
		DataSet <Row> filter = in.select(new String[] {this.getLabelCol(), this.getPredictionCol()})
			.getDataSet()
			.filter(new FilterFunction <Row>() {
				private static final long serialVersionUID = -3256594120752618106L;

				@Override
				public boolean filter(Row value) throws Exception {
					return EvaluationUtil.checkRowFieldNotNull(value);
				}
			});

		DataSet <Row> out = filter
			.mapPartition(new CalcLocal())
			.withBroadcastSet(DataSetUtil.count(filter), "count")
			.reduce(new EvaluationUtil.ReduceBaseMetrics())
			.flatMap(new EvaluationUtil.SaveDataAsParams());

		this.setOutputTable(DataSetConversionUtil.toTable(getMLEnvironmentId(),
			out, new TableSchema(new String[] {"regression_eval_result"}, new TypeInformation[] {Types.STRING})
		));
		return this;
	}

	/**
	 * Get the label sum, predResult sum, SSE, MAE, MAPE of one partition.
	 */
	public static class CalcLocal extends RichMapPartitionFunction <Row, BaseMetricsSummary> {
		private static final long serialVersionUID = -8330929147755146438L;

		@Override
		public void open(Configuration param) {
			long count = (long) getRuntimeContext().getBroadcastVariable("count").get(0);
			AkPreconditions.checkState(count > 0,
				new AkIllegalDataException("Please check the evaluation input! there is no effective row!"));
		}

		@Override
		public void mapPartition(Iterable <Row> rows, Collector <BaseMetricsSummary> collector)
			throws Exception {
			collector.collect(getRegressionStatistics(rows));
		}
	}

	@Override
	public RegressionMetrics createMetrics(List <Row> rows) {
		return new RegressionMetrics(rows.get(0));
	}
}
