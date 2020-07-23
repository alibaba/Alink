package com.alibaba.alink.operator.common.feature;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class EqualWidthDiscretizerModelInfoBatchOp
    extends ExtractModelInfoBatchOp<EqualWidthDiscretizerModelInfoBatchOp.EqualWidthDiscretizerModelInfo,
    EqualWidthDiscretizerModelInfoBatchOp> {

    public EqualWidthDiscretizerModelInfoBatchOp() {
        this(null);
    }

    public EqualWidthDiscretizerModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    public EqualWidthDiscretizerModelInfo createModelInfo(List<Row> rows) {
        return new EqualWidthDiscretizerModelInfo(rows);
    }

    /**
     * Summary of EqualWidth Discretizer Model;
     */
    public static class EqualWidthDiscretizerModelInfo extends QuantileDiscretizerModelInfo {
        public EqualWidthDiscretizerModelInfo(List<Row> list) {
            super(list);
        }
    }
}
