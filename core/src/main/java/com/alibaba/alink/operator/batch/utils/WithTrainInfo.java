package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.common.lazy.LazyObjectsManager;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public interface WithTrainInfo<S, T extends BatchOperator <T>> {

	S createTrainInfo(List <Row> rows);

	BatchOperator <?> getSideOutputTrainInfo();

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
		Consumer <BatchOperator <?>> consumer = new Consumer <BatchOperator <?>>() {
			@Override
			public void accept(BatchOperator <?> op) {
				((WithTrainInfo <?, ?>) op).getSideOutputTrainInfo().lazyCollect(d -> {
					S trainInfo = createTrainInfo(d);
					for (Consumer <S> callback : callbacks) {
						callback.accept(trainInfo);
					}
				});
			}
		};
		if (((T) this).isNullOutputTable()) {
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager((T) this);
			LazyEvaluation <BatchOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked((T) this);
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
