package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.dataproc.vector.VectorImputerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.common.utils.RowCollector;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;
import org.apache.flink.util.Collector;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Train will train a model for predict.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the input fillValue.
 * Or it will throw "no support" exception.
 */
public class VectorImputerTrainBatchOp extends BatchOperator<VectorImputerTrainBatchOp>
    implements VectorImputerTrainParams<VectorImputerTrainBatchOp> {

    public VectorImputerTrainBatchOp() {
        super(null);
    }

    public VectorImputerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorImputerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();
        String strategy = getStrategy();

        /* result is statistic model with strategy. */
        VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
        converter.vectorColName = vectorColName;

        /* if strategy is not min, max, mean, then only need to write the number. */
        DataSet<Row> rows;
        if (isNeedStatModel()) {
            /* first calculate the data, then transform it into model. */
            rows = StatisticsHelper.vectorSummary(in, vectorColName)
                .flatMap(new BuildVectorImputerModel(vectorColName, strategy));
        } else {
            String fillValue = getFillValue();
            RowCollector collector = new RowCollector();
            converter.save(Tuple2.of(fillValue, null), collector);
            rows = MLEnvironmentFactory.get(getMLEnvironmentId()).getExecutionEnvironment().fromCollection(collector.getRows());
        }
        this.setOutput(rows, converter.getModelSchema());
        return this;
    }

    private boolean isNeedStatModel() {
        String strategy = getStrategy();
        if ("min".equals(strategy) || "max".equals(strategy) || "mean".equals(strategy)) {
            return true;
        } else if ("value".equals(strategy)){
            return false;
        } else {
            throw new IllegalArgumentException("Only support \"max\", \"mean\", \"min\" and \"value\" strategy.");
        }
    }


    /**
     * table summary build model.
     */
    public static class BuildVectorImputerModel implements FlatMapFunction<BaseVectorSummary, Row> {
        private String selectedColName;
        private String strategy;

        public BuildVectorImputerModel(String selectedColName, String strategy) {
            this.selectedColName = selectedColName;
            this.strategy = strategy;
        }

        @Override
        public void flatMap(BaseVectorSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                VectorImputerModelDataConverter converter = new VectorImputerModelDataConverter();
                converter.vectorColName = selectedColName;

                converter.save(new Tuple2<>(strategy, srt), collector);
            }
        }
    }

}
