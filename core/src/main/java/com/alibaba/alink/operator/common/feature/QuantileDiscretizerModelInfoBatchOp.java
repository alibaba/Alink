package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * QuantileDiscretizerModelInfoBatchOp can be linked to the output of QuantileDiscretizerTrainBatchOp to summary the QuantileDiscretizer model.
 */
public class QuantileDiscretizerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<QuantileDiscretizerModelInfo, QuantileDiscretizerModelInfoBatchOp> {
    public QuantileDiscretizerModelInfoBatchOp() {
        this(null);
    }

    public QuantileDiscretizerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    public QuantileDiscretizerModelInfo createModelInfo(List<Row> rows) {
        return new QuantileDiscretizerModelInfo(rows);
    }
}
