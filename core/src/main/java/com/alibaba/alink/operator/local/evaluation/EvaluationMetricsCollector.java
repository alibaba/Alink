package com.alibaba.alink.operator.local.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.operator.local.LocalOperator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Collector the evaluation metrics to local.
 */
public interface EvaluationMetricsCollector<S, T extends LocalOperator <T>> {

	//S collectMetrics() ;
	//
	//default T printMetrics() {
	//	return printMetrics(null);
	//}
	//
	//default T printMetrics(String title) {
	//		if (null != title) {
	//			System.out.println(title);
	//		}
	//		System.out.println(collectMetrics());
	//		return (T)this;
	//}


	S createMetrics(List <Row> rows);

	default T lazyPrintMetrics() {
		return lazyPrintMetrics(null);
	}

	default T lazyPrintMetrics(String title) {
		return lazyCollectMetrics(d -> {
			if (null != title) {
				System.out.println(title);
			}
			System.out.println(d);
		});
	}

	default T lazyCollectMetrics(Consumer <S>... callbacks) {
		return lazyCollectMetrics(Arrays.asList(callbacks));
	}

	default T lazyCollectMetrics(List <Consumer <S>> callbacks) {
		((T) this).lazyCollect(d -> {
			S summary = createMetrics(d);
			for (Consumer <S> callback : callbacks) {
				callback.accept(summary);
			}
		});

		return ((T) this);
	}

	default S collectMetrics() {
		List <Row> list = ((T) this).collect();
		AkPreconditions.checkState(list.size() > 0, "There is no data in evaluation result");
		return createMetrics(list);
	}
}
