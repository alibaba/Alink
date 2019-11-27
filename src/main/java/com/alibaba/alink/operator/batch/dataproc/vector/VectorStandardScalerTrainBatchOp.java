package com.alibaba.alink.operator.batch.dataproc.vector;

import com.alibaba.alink.operator.common.dataproc.vector.VectorStandardScalerModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;
import org.apache.flink.util.Collector;

/**
 * StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 * If withMean is false, set mean as 0; if withStd is false, set std as 1.
 */
public final class VectorStandardScalerTrainBatchOp extends BatchOperator<VectorStandardScalerTrainBatchOp>
    implements VectorStandardTrainParams<VectorStandardScalerTrainBatchOp> {

    public VectorStandardScalerTrainBatchOp() {
        this(new Params());
    }

    public VectorStandardScalerTrainBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorStandardScalerTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();

        VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
        converter.vectorColName = vectorColName;

        DataSet<Row> rows = StatisticsHelper.vectorSummary(in, vectorColName)
            .flatMap(new BuildVectorStandardModel(vectorColName, getWithMean(), getWithStd()));

        setOutput(rows, converter.getModelSchema());

        return this;
    }

    /**
     * table summary build model.
     */
    public static class BuildVectorStandardModel implements FlatMapFunction<BaseVectorSummary, Row> {
        private String selectedColName;
        private boolean withMean;
        private boolean withStd;

        public BuildVectorStandardModel(String selectedColName, boolean withMean, boolean withStd) {
            this.selectedColName = selectedColName;
            this.withMean = withMean;
            this.withStd = withStd;
        }

        @Override
        public void flatMap(BaseVectorSummary srt, Collector<Row> collector) throws Exception {
            if (null != srt) {
                VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
                converter.vectorColName = selectedColName;

                converter.save(Tuple3.of(withMean, withStd, srt), collector);
            }
        }
    }

}
