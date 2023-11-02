package com.alibaba.alink.operator.stream.evaluation;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.stream.utils.TimeUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.evaluation.Regression;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.common.type.AlinkTypes;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.evaluation.EvalRegressionStreamParams;
import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Calculate the evaluation data within time windows for regression.
 * The evaluation metrics are:
 * SST: Sum of Squared for Total
 * SSE: Sum of Squares for Error
 * SSR: Sum of Squares for Regression
 * R^2: Coefficient of Determination
 * R: Multiple CorrelationBak Coeffient
 * MSE: Mean Squared Error
 * RMSE: Root Mean Squared Error
 * SAE/SAD: Sum of Absolute Error/Difference
 * MAE/MAD: Mean Absolute Error/Difference
 * MAPE: Mean Absolute Percentage Error
 */
@NameCn("流式回归评估组件")
@NameEn("Evaluation for Regression")
public class EvalRegressionStreamOp extends StreamOperator <EvalRegressionStreamOp>
	implements EvalRegressionStreamParams<EvalRegressionStreamOp> {
	private static final long serialVersionUID = 295427507536024445L;

	public EvalRegressionStreamOp() {
		super(null);
	}

	public EvalRegressionStreamOp(Params params) {
		super(params);
	}

	@Override
	public EvalRegressionStreamOp linkFrom(StreamOperator<?>... inputs) {
		StreamOperator<?> in = checkAndGetFirst(inputs);
		TableUtil.assertNumericalCols(in.getSchema(), this.getLabelCol(), this.getPredictionCol());

		Regression regressionEvaluationWindowFunction = new Regression();
		DataStream <Row> res = in.select(this.getLabelCol() + ", " + this.getPredictionCol()).getDataStream()
			.windowAll(TumblingProcessingTimeWindows.of(TimeUtil.convertTime(this.getTimeInterval())))
			.apply(regressionEvaluationWindowFunction);

		this.setOutputTable(DataStreamConversionUtil.toTable(getMLEnvironmentId(), res,
			new TableSchema(new String[] {"regression_eval_result"}, new TypeInformation[] {AlinkTypes.STRING})));
		return this;
	}
}
