package com.alibaba.alink.operator.common.clustering.lda;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class LdaModelInfoBatchOp extends ExtractModelInfoBatchOp<LdaModelInfo, LdaModelInfoBatchOp> {

    public LdaModelInfoBatchOp() {
        this(null);
    }

    public LdaModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected LdaModelInfo createModelInfo(List<Row> rows) {
        return new LdaModelInfo(rows);
    }
}
