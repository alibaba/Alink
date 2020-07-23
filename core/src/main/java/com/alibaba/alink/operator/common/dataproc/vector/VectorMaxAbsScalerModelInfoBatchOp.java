package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class VectorMaxAbsScalerModelInfoBatchOp extends
    ExtractModelInfoBatchOp<VectorMaxAbsScalarModelInfo, VectorMaxAbsScalerModelInfoBatchOp> {

    public VectorMaxAbsScalerModelInfoBatchOp() {
        this(null);
    }

    public VectorMaxAbsScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected VectorMaxAbsScalarModelInfo createModelInfo(List<Row> rows) {
        return new VectorMaxAbsScalarModelInfo(rows);
    }
}
