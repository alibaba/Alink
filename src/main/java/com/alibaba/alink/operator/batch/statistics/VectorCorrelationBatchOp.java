package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.SpearmanCorrelation;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.params.statistics.VectorCorrelationParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Calculating the correlation between two series of data is a common operation in Statistics.
 */
public final class VectorCorrelationBatchOp extends BatchOperator<VectorCorrelationBatchOp>
    implements VectorCorrelationParams<VectorCorrelationBatchOp> {

    public VectorCorrelationBatchOp() {
        super(null);
    }

    public VectorCorrelationBatchOp(Params params) {
        super(params);
    }

    @Override
    public VectorCorrelationBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String vectorColName = getSelectedCol();

        String corrType = getMethod().trim().toLowerCase();

        if ("pearson".equals(corrType)) {
            DataSet<Tuple2<BaseVectorSummary, CorrelationResult>> srt = StatisticsHelper.vectorPearsonCorrelation(in, vectorColName);

            //block
            DataSet<Row> result = srt
                .flatMap(new FlatMapFunction<Tuple2<BaseVectorSummary, CorrelationResult>, Row>() {
                    @Override
                    public void flatMap(Tuple2<BaseVectorSummary, CorrelationResult> srt, Collector<Row> collector) throws Exception {
                        new CorrelationDataConverter().save(srt.f1, collector);
                    }
                });

            this.setOutput(result, new CorrelationDataConverter().getModelSchema());

        } else {

            DataSet<Row> data = StatisticsHelper.transformToColumns(in, null, vectorColName, null);

            DataSet<Row> rank = SpearmanCorrelation.calcRank(data, true);

            BatchOperator rankOp = new TableSourceBatchOp(DataSetConversionUtil.toTable(getMLEnvironmentId(), rank,
                new String[]{"col"}, new TypeInformation[]{Types.STRING}))
                .setMLEnvironmentId(getMLEnvironmentId());

            VectorCorrelationBatchOp corrBatchOp = new VectorCorrelationBatchOp()
                .setMLEnvironmentId(getMLEnvironmentId())
                .setSelectedCol("col");

            rankOp.link(corrBatchOp);

            this.setOutput(corrBatchOp.getDataSet(), corrBatchOp.getSchema());
        }
        return this;
    }

    public CorrelationResult collectCorrelation() {
        Preconditions.checkArgument(null != this.getOutputTable(), "Please link from or link to.");
        return new CorrelationDataConverter().load(this.collect());
    }
}








