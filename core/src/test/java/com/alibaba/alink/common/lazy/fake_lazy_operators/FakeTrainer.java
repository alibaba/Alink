package com.alibaba.alink.common.lazy.fake_lazy_operators;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.pipeline.Trainer;

public class FakeTrainer extends Trainer<FakeTrainer, FakeModel>
        implements HasLazyPrintTrainInfo<FakeTrainer>, HasLazyPrintModelInfo<FakeTrainer> {
    @Override
    protected BatchOperator train(BatchOperator in) {
        return new FakeOperator(getParams()).linkFrom(in);
    }
}
