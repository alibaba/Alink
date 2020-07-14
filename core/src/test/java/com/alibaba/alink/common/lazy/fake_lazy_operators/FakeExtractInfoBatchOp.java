package com.alibaba.alink.common.lazy.fake_lazy_operators;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

public class FakeExtractInfoBatchOp extends ExtractModelInfoBatchOp<FakeModelInfo, FakeExtractInfoBatchOp> {

    public FakeExtractInfoBatchOp() {
        super(null);
    }

    public FakeExtractInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    protected FakeModelInfo createModelInfo(List<Row> rows) {
        return new FakeModelInfo(rows);
    }
}
