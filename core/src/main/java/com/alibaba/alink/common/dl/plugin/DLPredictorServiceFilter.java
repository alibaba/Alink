package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.io.plugin.RegisterKey;

import java.util.function.Predicate;

class DLPredictorServiceFilter implements Predicate <DLPredictorService> {
	private final RegisterKey registerKey;

	public DLPredictorServiceFilter(RegisterKey registerKey) {
		this.registerKey = registerKey;
	}

	@Override
	public boolean test(DLPredictorService service) {
		return true;
	}
}
