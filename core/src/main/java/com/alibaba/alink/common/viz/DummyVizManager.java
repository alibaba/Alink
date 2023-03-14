package com.alibaba.alink.common.viz;

import org.apache.flink.ml.api.misc.param.Params;

public class DummyVizManager implements VizManagerInterface {

	private static final long serialVersionUID = 8639356595306220989L;

	@Override
	public VizDataWriterInterface getVizDataWriter(Params params) {
		return new DummyVizDataWriter();
	}

	@Override
	public String getVizName() {
		return null;
	}
}
