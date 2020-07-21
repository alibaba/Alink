package com.alibaba.alink.operator.common.linear;

import java.util.List;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

public class LinearRegressorModelInfoBatchOp
    extends ExtractModelInfoBatchOp<LinearRegressorModelInfo, LinearRegressorModelInfoBatchOp> {

    public LinearRegressorModelInfoBatchOp() {
        this(null);
    }

    public LinearRegressorModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected LinearRegressorModelInfo createModelInfo(List<Row> rows) {
        return new LinearRegressorModelInfo(rows);
    }

    @Override
    protected BatchOperator<?> processModel() {
        return this;
    }
}
