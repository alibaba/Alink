package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.evaluation.*;
import com.alibaba.alink.params.evaluation.RegressionEvaluationParams;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getRegressionStatistics;

/**
 * Calculate the evaluation data for regression. The evaluation metrics are: SST: Sum of Squared for Total SSE: Sum of
 * Squares for Error SSR: Sum of Squares for Regression R^2: Coefficient of Determination R: Multiple CorrelationBak
 * Coeffient MSE: Mean Squared Error RMSE: Root Mean Squared Error SAE/SAD: Sum of Absolute Error/Difference MAE/MAD:
 * Mean Absolute Error/Difference MAPE: Mean Absolute Percentage Error
 */
public final class EvalRegressionBatchOp extends BatchOperator<EvalRegressionBatchOp>
    implements RegressionEvaluationParams<EvalRegressionBatchOp>, EvaluationMetricsCollector<RegressionMetrics> {

    public EvalRegressionBatchOp() {
        super(null);
    }

    public EvalRegressionBatchOp(Params params) {
        super(params);
    }

    @Override
    public EvalRegressionBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator in = checkAndGetFirst(inputs);

        int indexLabel = TableUtil.findColIndex(in.getColNames(), this.getLabelCol());
        int indexPredict = TableUtil.findColIndex(in.getColNames(), this.getPredictionCol());
        Preconditions.checkArgument(indexLabel >= 0 && indexPredict >= 0, "Can not find given columns!");

        TableUtil.assertNumericalCols(in.getSchema(), this.getLabelCol(), this.getPredictionCol());
        DataSet<Row> out = in.select(new String[] {this.getLabelCol(), this.getPredictionCol()})
            .getDataSet()
            .rebalance()
            .mapPartition(new CalcLocal())
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
    public static class CalcLocal implements MapPartitionFunction<Row, BaseMetricsSummary> {
        @Override
        public void mapPartition(Iterable<Row> rows, Collector<BaseMetricsSummary> collector)
            throws Exception {
            collector.collect(getRegressionStatistics(rows));
        }
    }

    @Override
    public RegressionMetrics collectMetrics() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please provide the dataset to evaluate!");
        return new RegressionMetrics(this.collect().get(0));
    }
}
