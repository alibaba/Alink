package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class StandardScalerModelInfoBatchOp extends
    ExtractModelInfoBatchOp<StandardScalerModelInfo, StandardScalerModelInfoBatchOp> {

    public StandardScalerModelInfoBatchOp() {
        this(null);
    }

    public StandardScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected StandardScalerModelInfo createModelInfo(List<Row> rows) {
        return new StandardScalerModelInfo(rows);
    }
}
