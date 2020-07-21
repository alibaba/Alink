package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * OneHotModelInfoBatchOp can be linked to the output of OneHotTrainBatchOp to summary the OneHot model.
 */
public class OneHotModelInfoBatchOp extends ExtractModelInfoBatchOp<OneHotModelInfo, OneHotModelInfoBatchOp> {
    public OneHotModelInfoBatchOp() {
        this(null);
    }

    public OneHotModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    public OneHotModelInfo createModelInfo(List<Row> rows) {
        return new OneHotModelInfo(rows);
    }
}
