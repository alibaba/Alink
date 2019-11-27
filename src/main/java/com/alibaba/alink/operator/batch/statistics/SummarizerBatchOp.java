package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SummaryDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.statistics.SummarizerParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * It is summary of table, support count, mean, variance, min, max, sum.
 */
public class SummarizerBatchOp extends BatchOperator<SummarizerBatchOp>
    implements SummarizerParams<SummarizerBatchOp> {

    /**
     * default constructor.
     */
    public SummarizerBatchOp() {
        super(null);
    }

    /**
     * constructor with params.
     */
    public SummarizerBatchOp(Params params) {
        super(params);
    }

    @Override
    public SummarizerBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(1, inputs);
        BatchOperator<?> in = inputs[0];

        String[] selectedColNames = in.getColNames();
        if (this.getParams().contains(SummarizerParams.SELECTED_COLS)) {
            selectedColNames = this.getParams().get(SummarizerParams.SELECTED_COLS);
        }

        DataSet<TableSummary> srt = StatisticsHelper.summary(in, selectedColNames);


        //result may result.
        DataSet<Row> out = srt
            .flatMap(new TableSummaryBuildModel());

        SummaryDataConverter converter = new SummaryDataConverter();

        this.setOutput(out, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class TableSummaryBuildModel implements FlatMapFunction<TableSummary, Row> {

        TableSummaryBuildModel() {

        }

        @Override
        public void flatMap(TableSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                SummaryDataConverter modelConverter = new SummaryDataConverter();

                modelConverter.save(srt, collector);
            }
        }
    }

    public TableSummary collectSummary() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return new SummaryDataConverter().load(this.collect());
    }

}
