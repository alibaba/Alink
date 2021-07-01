package com.alibaba.alink.operator.common.prophet;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * the prophet model info batch op
 */
public class ProphetModelInfoBatchOp extends ExtractModelInfoBatchOp<ProphetModelInfo, ProphetModelInfoBatchOp> {

    public ProphetModelInfoBatchOp() {
        this(null);
    }

    public ProphetModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected ProphetModelInfo createModelInfo(List<Row> list) {
        return new ProphetModelInfo(list);
    }
}
