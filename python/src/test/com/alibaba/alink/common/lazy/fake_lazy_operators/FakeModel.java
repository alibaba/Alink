package com.alibaba.alink.common.lazy.fake_lazy_operators;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.pipeline.MapModel;

public class FakeModel extends MapModel <FakeModel> {
	public FakeModel() {
		this(new Params());
	}

	public FakeModel(Params params) {
		super(FakeModelMapper::new, params);
	}
}
