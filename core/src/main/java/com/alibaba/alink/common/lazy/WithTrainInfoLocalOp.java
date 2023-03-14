package com.alibaba.alink.common.lazy;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.lazy.LocalLazyObjectsManager;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public interface WithTrainInfoLocalOp<S, T extends LocalOperator <T>> {

	S createTrainInfo(List <Row> rows);

	LocalOperator <?> getSideOutputTrainInfo();

	default T lazyPrintTrainInfo(String title) {
		return lazyCollectTrainInfo(d -> {
			if (null != title) {
				System.out.println(title);
			}
			System.out.println(d);
		});
	}

	default T lazyPrintTrainInfo() {
		return lazyPrintTrainInfo(null);
	}

	default T lazyCollectTrainInfo(List <Consumer <S>> callbacks) {
		Consumer <LocalOperator <?>> consumer = new Consumer <LocalOperator <?>>() {
			@Override
			public void accept(LocalOperator <?> op) {
				((WithTrainInfoLocalOp <?, ?>) op).getSideOutputTrainInfo().lazyCollect(d -> {
					S trainInfo = createTrainInfo(d);
					for (Consumer <S> callback : callbacks) {
						callback.accept(trainInfo);
					}
				});
			}
		};
		if (((T) this).isNullOutputTable()) {
			LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager((T) this);
			LazyEvaluation <LocalOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked((T) this);
			lazyOpAfterLinked.addCallback(consumer);
		} else {
			consumer.accept((T) this);
		}
		return ((T) this);
	}

	default T lazyCollectTrainInfo(Consumer <S>... callbacks) {
		return lazyCollectTrainInfo(Arrays.asList(callbacks));
	}

	default S collectTrainInfo() {
		return createTrainInfo(getSideOutputTrainInfo().collect());
	}
}
