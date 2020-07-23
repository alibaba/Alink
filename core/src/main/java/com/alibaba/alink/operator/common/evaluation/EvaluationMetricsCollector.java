package com.alibaba.alink.operator.common.evaluation;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Collector the evaluation metrics to local.
 */
public interface EvaluationMetricsCollector<S, T extends BatchOperator<T>> {

    S createMetrics(List<Row> rows);

    default T lazyPrintMetrics() {
        return lazyCollectMetrics(System.out::println);
    }

    default T lazyCollectMetrics(Consumer<S>... callbacks) {
        return lazyCollectMetrics(Arrays.asList(callbacks));
    }

    default T lazyCollectMetrics(List<Consumer<S>> callbacks) {
        ((T)this).lazyCollect(d -> {
            S summary = createMetrics(d);
            for (Consumer <S> callback : callbacks) {
                callback.accept(summary);
            }
        });

        return ((T) this);
    }

    default S collectMetrics() {
        List<Row> list = ((T)this).collect();
        Preconditions.checkArgument(list.size() > 0, "There is no data in evaluation result");
        return createMetrics(list);
    }
}
