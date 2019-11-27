package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerTrainParams;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * MaxAbsScaler transforms a dataSet of Vector rows, rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 * MaxAbsScalerTrain will train a model.
 */
public final class VectorMaxAbsScalerTrainBatchOp extends BatchOperator<VectorMaxAbsScalerTrainBatchOp>
    implements VectorMaxAbsScalerTrainParams<VectorMaxAbsScalerTrainBatchOp> {

    public VectorMaxAbsScalerTrainBatchOp() {
        this(new Params());
    }

    public VectorMaxAbsScalerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorMaxAbsScalerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();

        VectorMaxAbsScalerModelDataConverter converter = new VectorMaxAbsScalerModelDataConverter();
        converter.vectorColName = vectorColName;

        DataSet<Row> rows = StatisticsHelper.vectorSummary(in, vectorColName)
            .flatMap(new BuildVectorMaxAbsModel(vectorColName));

        setOutput(rows, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class BuildVectorMaxAbsModel implements FlatMapFunction<BaseVectorSummary, Row> {
        private String selectedColName;

        BuildVectorMaxAbsModel(String selectedColName) {
            this.selectedColName = selectedColName;
        }

        @Override
        public void flatMap(BaseVectorSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                VectorMaxAbsScalerModelDataConverter converter = new VectorMaxAbsScalerModelDataConverter();
                converter.vectorColName = selectedColName;

                converter.save(srt, collector);
            }
        }
    }


}
