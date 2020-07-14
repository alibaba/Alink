package com.alibaba.alink.common.lazy.fake_lazy_operators;

import com.alibaba.alink.pipeline.MapModel;
import org.apache.flink.ml.api.misc.param.Params;

public class FakeModel extends MapModel<FakeModel> {
    public FakeModel() {
        this(new Params());
    }

    public FakeModel(Params params) {
        super(FakeModelMapper::new, params);
    }
}
