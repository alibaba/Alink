package com.alibaba.alink.operator.common.dataproc.vector;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class VectorMinMaxScalerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<VectorMinMaxScalerModelInfo, VectorMinMaxScalerModelInfoBatchOp> {

    public VectorMinMaxScalerModelInfoBatchOp() {
        this(null);
    }

    public VectorMinMaxScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected VectorMinMaxScalerModelInfo createModelInfo(List<Row> rows) {
        return new VectorMinMaxScalerModelInfo(rows);
    }
}
