package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class VectorStandardScalerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<VectorStandardScalerModelInfo, VectorStandardScalerModelInfoBatchOp> {

    public VectorStandardScalerModelInfoBatchOp() {
        this(null);
    }

    public VectorStandardScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected VectorStandardScalerModelInfo createModelInfo(List<Row> rows) {
        return new VectorStandardScalerModelInfo(rows);
    }
}
