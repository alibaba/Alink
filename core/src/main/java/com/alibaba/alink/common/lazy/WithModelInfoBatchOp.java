package com.alibaba.alink.common.lazy;

import com.alibaba.alink.params.shared.HasMLEnvironmentId;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * An interface indicating a BatchOperator has its corresponding ExtractModelInfoBatchOp subclass,
 * so the BatchOperator can provide, collect, and print its model information.
 *
 * @param <S> the BatchOperator class which produces a model.
 * @param <T> the class which conveys the model information.
 * @param <E> the matching subclass of ExtractModelInfoBatchOp to extract model information.
 * @see ExtractModelInfoBatchOp
 */
public interface WithModelInfoBatchOp<S, T, E extends ExtractModelInfoBatchOp<S, E>> {

    /**
     * Get an instance of the matching ExtractModelInfoBatchOp.
     * <p>
     * The ExtractModelInfoBatchOp instance should be linked from `this`.
     *
     * @return the instance of the subclass of ExtractModelInfoBatchOp.
     */
    E getModelInfoBatchOp();

    /**
     * Lazily print model information with a title.
     *
     * @param title title
     * @return the BatchOperator itself
     */
    default T lazyPrintModelInfo(String title) {
        getModelInfoBatchOp()
            .setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
            .lazyPrintModelInfo(title);
        return (T) this;
    }

    /**
     * Lazily print model information.
     *
     * @return the BatchOperator itself
     */
    default T lazyPrintModelInfo() {
        return lazyPrintModelInfo(null);
    }

    /**
     * Lazily trigger callbacks for model information.
     *
     * @param callbacks callbacks
     * @return the BatchOperator itself
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    default T lazyCollectModelInfo(List<Consumer<S>> callbacks) {
        getModelInfoBatchOp()
            .setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
            .lazyCollectModelInfo(callbacks);
        return (T) this;
    }

    default T lazyCollectModelInfo(Consumer<S>... callbacks) {
        return lazyCollectModelInfo(Arrays.asList(callbacks));
    }

    /**
     * Collect the model information.
     *
     * @return the BatchOperator itself
     */
    @SuppressWarnings("rawtypes")
    default S collectModelInfo() {
        return getModelInfoBatchOp()
            .setMLEnvironmentId(((HasMLEnvironmentId) this).getMLEnvironmentId())
            .collectModelInfo();
    }
}
