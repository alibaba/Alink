package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.operator.common.dataproc.vector.VectorMinMaxScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;
import org.apache.flink.util.Collector;

/**
 * MinMaxScaler transforms a dataSet of rows, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 * MinMaxScalerTrain will train a model.
 */
public final class VectorMinMaxScalerTrainBatchOp extends BatchOperator<VectorMinMaxScalerTrainBatchOp>
    implements VectorMinMaxScalerTrainParams<VectorMinMaxScalerTrainBatchOp> {

    public VectorMinMaxScalerTrainBatchOp() {
        this(new Params());
    }

    public VectorMinMaxScalerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorMinMaxScalerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();

        VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
        converter.vectorColName = vectorColName;

        DataSet<Row> rows = StatisticsHelper.vectorSummary(in, vectorColName)
            .flatMap(new BuildVectorMinMaxModel(vectorColName, getMin(), getMax()));

        setOutput(rows, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class BuildVectorMinMaxModel implements FlatMapFunction<BaseVectorSummary, Row> {
        private String selectedColName;
        private double min;
        private double max;

        public BuildVectorMinMaxModel(String selectedColName, double min, double max) {
            this.selectedColName = selectedColName;
            this.min = min;
            this.max = max;
        }

        @Override
        public void flatMap(BaseVectorSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
                converter.vectorColName = selectedColName;

                converter.save(Tuple3.of(min, max, srt), collector);
            }
        }
    }


}
