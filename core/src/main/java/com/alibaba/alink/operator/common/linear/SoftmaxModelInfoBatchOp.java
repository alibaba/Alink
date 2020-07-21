package com.alibaba.alink.operator.common.linear;

import java.util.List;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

public class SoftmaxModelInfoBatchOp
    extends ExtractModelInfoBatchOp<SoftmaxModelInfo, SoftmaxModelInfoBatchOp> {

    public SoftmaxModelInfoBatchOp() {
        this(null);
    }

    public SoftmaxModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected SoftmaxModelInfo createModelInfo(List<Row> rows) {
        return new SoftmaxModelInfo(rows);
    }

    @Override
    protected BatchOperator<?> processModel() {
        return this;
    }
}
