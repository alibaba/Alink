package com.alibaba.alink.operator.local.lazy;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public interface WithModelInfoLocalOp<S, T, E extends ExtractModelInfoLocalOp <S, E>> {

	E getModelInfoLocalOp();

	default T lazyPrintModelInfo(String title) {
		getModelInfoLocalOp()
			.lazyPrintModelInfo(title);
		//noinspection unchecked
		return (T) this;
	}

	default T lazyPrintModelInfo() {
		return lazyPrintModelInfo(null);
	}

	@SuppressWarnings({"unchecked"})
	default T lazyCollectModelInfo(List <Consumer <S>> callbacks) {
		getModelInfoLocalOp()
			.lazyCollectModelInfo(callbacks);
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	default T lazyCollectModelInfo(Consumer <S>... callbacks) {
		return lazyCollectModelInfo(Arrays.asList(callbacks));
	}

	default S collectModelInfo() {
		return getModelInfoLocalOp()
			.collectModelInfo();
	}
}
