package com.alibaba.alink.common.lazy;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * An interface indicating the information a BatchOperator can provide for its training process.
 *
 * @param <S> the class which conveys the train information.
 * @param <T> the BatchOperator class which provides the train information.
 */
public interface WithTrainInfo<S, T extends BatchOperator<T>> {

    /**
     * Create train information from data.
     *
     * @param rows data
     * @return the train information
     */
    S createTrainInfo(List<Row> rows);

    /**
     * Get the side output representing the train information of the BatchOperator.
     *
     * @return a BatchOperator representing the train information.
     */
    BatchOperator<?> getSideOutputTrainInfo();

    /**
     * Lazily print train information with a title.
     *
     * @param title title
     * @return the BatchOperator itself
     */
    default T lazyPrintTrainInfo(String title) {
        return lazyCollectTrainInfo(d -> {
            if (null != title) {
                System.out.println(title);
            }
            System.out.println(d);
        });
    }

    /**
     * Lazily print train information.
     *
     * @return the BatchOperator itself
     */
    default T lazyPrintTrainInfo() {
        return lazyPrintTrainInfo(null);
    }

    /**
     * Lazily trigger callbacks for train information when it is available.
     *
     * @param callbacks callbacks
     * @return the BatchOperator itself
     */
    default T lazyCollectTrainInfo(List<Consumer<S>> callbacks) {
        getSideOutputTrainInfo().lazyCollect(d -> {
            S trainInfo = createTrainInfo(d);
            for (Consumer<S> callback : callbacks) {
                callback.accept(trainInfo);
            }
        });

        return ((T) this);
    }

    default T lazyCollectTrainInfo(Consumer<S>... callbacks) {
        return lazyCollectTrainInfo(Arrays.asList(callbacks));
    }

    /**
     * Collect the train information.
     *
     * @return train information
     */
    default S collectTrainInfo() {
        return createTrainInfo(getSideOutputTrainInfo().collect());
    }
}
