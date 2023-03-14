package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.params.shared.HasMLEnvironmentId;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public interface WithModelInfoBatchOp<S, T, E extends ExtractModelInfoBatchOp <S, E>> {

	E getModelInfoBatchOp();

	default T lazyPrintModelInfo(String title) {
		getModelInfoBatchOp()
			.setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
			.lazyPrintModelInfo(title);
		return (T) this;
	}

	default T lazyPrintModelInfo() {
		return lazyPrintModelInfo(null);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	default T lazyCollectModelInfo(List <Consumer <S>> callbacks) {
		getModelInfoBatchOp()
			.setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
			.lazyCollectModelInfo(callbacks);
		return (T) this;
	}

	default T lazyCollectModelInfo(Consumer <S>... callbacks) {
		return lazyCollectModelInfo(Arrays.asList(callbacks));
	}

	@SuppressWarnings("rawtypes")
	default S collectModelInfo() {
		return getModelInfoBatchOp()
			.setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
			.collectModelInfo();
	}
}
