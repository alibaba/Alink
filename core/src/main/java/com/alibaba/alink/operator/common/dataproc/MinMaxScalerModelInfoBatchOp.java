package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class MinMaxScalerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<MinMaxScalerModelInfo, MinMaxScalerModelInfoBatchOp> {

    public MinMaxScalerModelInfoBatchOp() {
        this(null);
    }

    public MinMaxScalerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected MinMaxScalerModelInfo createModelInfo(List<Row> rows) {
        return new MinMaxScalerModelInfo(rows);
    }
}
