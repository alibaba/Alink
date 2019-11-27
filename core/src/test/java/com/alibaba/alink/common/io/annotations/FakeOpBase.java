package com.alibaba.alink.common.io.annotations;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;

/**
 * An simple helper class to simplify creation of fake Operator.
 *
 */
public class FakeOpBase extends BaseSourceBatchOp<FakeOpBase> {

    FakeOpBase(Params params) {
        super("", new Params());
    }

    @Override
    protected Table initializeDataSource() {
        return null;
    }

    @Override
    public Long getMLEnvironmentId() {
        return null;
    }

    @Override
    public FakeOpBase setMLEnvironmentId(Long value) {
        return null;
    }

    @Override
    public <V> FakeOpBase set(ParamInfo<V> info, V value) {
        return null;
    }

    @Override
    public <V> V get(ParamInfo<V> info) {
        return null;
    }
}
