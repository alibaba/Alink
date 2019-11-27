package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SpearmanCorrelation;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.statistics.CorrelationParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Calculating the correlation between two series of data is a common operation in Statistics.
 */
public final class CorrelationBatchOp extends BatchOperator<CorrelationBatchOp>
    implements CorrelationParams<CorrelationBatchOp> {

    public CorrelationBatchOp() {
        super(null);
    }

    public CorrelationBatchOp(Params params) {
        super(params);
    }

    @Override
    public CorrelationBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        String[] selectedColNames = this.getParams().get(SELECTED_COLS);

        if (selectedColNames == null) {
            selectedColNames = in.getColNames();
        }

        //check col types must be double or bigint
        TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

        String corrType = getMethod().trim().toLowerCase();

        if ("pearson".equals(corrType)) {

            DataSet<Tuple2<TableSummary, CorrelationResult>> srt = StatisticsHelper.pearsonCorrelation(in, selectedColNames);

            DataSet<Row> result = srt.
                flatMap(new FlatMapFunction<Tuple2<TableSummary, CorrelationResult>, Row>() {
                    @Override
                    public void flatMap(Tuple2<TableSummary, CorrelationResult> summary, Collector<Row> collector) {
                        new CorrelationDataConverter().save(summary.f1, collector);
                    }
                });


            this.setOutput(result, new CorrelationDataConverter().getModelSchema());
        } else {

            DataSet<Row> data = inputs[0].select(selectedColNames).getDataSet();
            DataSet<Row> rank = SpearmanCorrelation.calcRank(data, false);

            TypeInformation[] colTypes = new TypeInformation[selectedColNames.length];
            for (int i = 0; i < colTypes.length; i++) {
                colTypes[i] = Types.DOUBLE;
            }

            BatchOperator rankOp = new TableSourceBatchOp(DataSetConversionUtil.toTable(getMLEnvironmentId(), rank, selectedColNames, colTypes))
                .setMLEnvironmentId(getMLEnvironmentId());

            CorrelationBatchOp corrBatchOp = new CorrelationBatchOp()
                .setMLEnvironmentId(getMLEnvironmentId())
                .setSelectedCols(selectedColNames);

            rankOp.link(corrBatchOp);

            this.setOutput(corrBatchOp.getDataSet(), corrBatchOp.getSchema());

        }

        return this;
    }


    public CorrelationResult collectCorrelationResult() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return new CorrelationDataConverter().load(this.collect());
    }
}








