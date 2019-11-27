package com.alibaba.alink.operator.batch.dataproc;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.common.dataproc.ImputerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.common.utils.RowCollector;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;
import org.apache.flink.util.Collector;

/**
 * Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 * Imputer Train will train a model for predict.
 * Strategy support min, max, mean or value.
 * If min, will replace missing value with min of the column.
 * If max, will replace missing value with max of the column.
 * If mean, will replace missing value with mean of the column.
 * If value, will replace missing value with the value.
 */
public class ImputerTrainBatchOp extends BatchOperator<ImputerTrainBatchOp>
    implements ImputerTrainParams<ImputerTrainBatchOp> {

    public ImputerTrainBatchOp() {
        super(null);
    }

    public ImputerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public ImputerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();
        String strategy = getStrategy();

        //result is statistic model with strategy.
        ImputerModelDataConverter converter = new ImputerModelDataConverter();
        converter.selectedColNames = selectedColNames;
        converter.selectedColTypes = TableUtil.findColTypes(in.getSchema(), selectedColNames);

        Params meta = new Params()
            .set(ImputerTrainParams.STRATEGY, strategy);

        //if strategy is not min, max, mean
        DataSet<Row> rows;
        if (isNeedStatModel()) {
            rows = StatisticsHelper.summary(in, selectedColNames)
                .flatMap(new BuildImputerModel(selectedColNames,
                        TableUtil.findColTypes(in.getSchema(), selectedColNames), strategy));

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
    public static class BuildImputerModel implements FlatMapFunction<TableSummary, Row> {
        private String[] selectedColNames;
        private TypeInformation[] selectedColTypes;
        private String strategy;

        public BuildImputerModel(String[] selectedColNames, TypeInformation[] selectedColTypes, String strategy) {
            this.selectedColNames = selectedColNames;
            this.selectedColTypes = selectedColTypes;
            this.strategy = strategy;
        }

        @Override
        public void flatMap(TableSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                ImputerModelDataConverter converter = new ImputerModelDataConverter();
                converter.selectedColNames = selectedColNames;
                converter.selectedColTypes = selectedColTypes;

                converter.save(new Tuple2<>(strategy, srt), collector);
            }
        }
    }

}
