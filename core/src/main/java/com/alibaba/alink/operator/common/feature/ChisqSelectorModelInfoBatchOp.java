package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Chisq selector model info.
 */
public class ChisqSelectorModelInfoBatchOp
    extends ExtractModelInfoBatchOp<ChisqSelectorModelInfo, ChisqSelectorModelInfoBatchOp> {

    public ChisqSelectorModelInfoBatchOp() {
        this(null);
    }

    public ChisqSelectorModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected ChisqSelectorModelInfo createModelInfo(List<Row> rows) {
        return new ChisqSelectorModelInfo(rows);
    }
}
