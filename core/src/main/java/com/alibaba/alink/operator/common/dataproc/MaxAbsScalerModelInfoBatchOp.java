package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class MaxAbsScalerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<MaxAbsScalarModelInfo, MaxAbsScalerModelInfoBatchOp> {

    public MaxAbsScalerModelInfoBatchOp() {
        this(null);
    }

    public MaxAbsScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected MaxAbsScalarModelInfo createModelInfo(List<Row> rows) {
        return new MaxAbsScalarModelInfo(rows);
    }
}
