package com.alibaba.alink.common.lazy.fake_lazy_operators;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.common.lazy.WithTrainInfo;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.List;

public class FakeOperator extends BatchOperator<FakeOperator>
        implements WithModelInfoBatchOp<FakeModelInfo, FakeOperator, FakeExtractInfoBatchOp>,
    WithTrainInfo<FakeTrainInfo, FakeOperator> {

    public FakeOperator() {
        super(null);
    }

    public FakeOperator(Params params) {
        super(params);
    }

    @Override
    public FakeOperator linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);

        DataSet<Row> trainInfo = in.getDataSet().map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                return Row.of(value.toString());
            }
        });

        setSideOutputTables(new Table[]{
                DataSetConversionUtil.toTable(getMLEnvironmentId(), trainInfo, new String[]{"info"}, new TypeInformation[]{Types.STRING})
        });
        setOutputTable(in.getOutputTable());
        return this;
    }

    @Override
    public FakeExtractInfoBatchOp getModelInfoBatchOp() {
        return new FakeExtractInfoBatchOp().linkFrom(this);
    }

    @Override
    public FakeTrainInfo createTrainInfo(List<Row> rows) {
        return new FakeTrainInfo(rows);
    }

    @Override
    public BatchOperator<?> getSideOutputTrainInfo() {
        return this;
    }
}
