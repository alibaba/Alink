package com.alibaba.alink.common.lazy;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * A base BatchOperator class to provide the function of model information extraction.
 *
 * @param <S> the class which conveys the model information.
 * @param <T> the BatchOperator class which produces a model.
 */
public abstract class ExtractModelInfoBatchOp<S, T extends ExtractModelInfoBatchOp<S, T>> extends BatchOperator<T> {
    public ExtractModelInfoBatchOp(Params params) {
        super(params);
    }

    @Override
    final public T linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> op = checkAndGetFirst(inputs);
        setOutputTable(op.getOutputTable());
        return (T) this;
    }

    /**
     * Create model information from data.
     *
     * @param rows data
     * @return the model information.
     */
    protected abstract S createModelInfo(List<Row> rows);

    /**
     * Process the model if needed.
     *
     * @return the processed model.
     */
    protected BatchOperator<?> processModel() {
        return this;
    }

    /**
     * Lazily print model information with a title.
     *
     * @param title title
     * @return the ExtractModelInfoBatchOp instance itself
     */
    final public T lazyPrintModelInfo(String title) {
        return lazyCollectModelInfo(d -> {
            if (null != title) {
                System.out.println(title);
            }
            System.out.println(d);
        });
    }

    /**
     * Lazily print model information with title.
     *
     * @return the ExtractModelInfoBatchOp instance itself
     */
    final public T lazyPrintModelInfo() {
        return lazyPrintModelInfo(null);
    }

    /**
     * Lazily trigger callbacks for model information when it is available.
     *
     * @param callbacks callbacks
     * @return the ExtractModelInfoBatchOp instance itself
     */
    final public T lazyCollectModelInfo(List<Consumer<S>> callbacks) {
        this.processModel().lazyCollect(d -> {
            S summary = createModelInfo(d);
            for (Consumer<S> callback : callbacks) {
                callback.accept(summary);
            }
        });
        return ((T) this);
    }

    final public T lazyCollectModelInfo(Consumer<S>... callbacks) {
        return lazyCollectModelInfo(Arrays.asList(callbacks));
    }

    /**
     * Collect the model information.
     *
     * @return model information
     */
    final public S collectModelInfo() {
        return createModelInfo(this.processModel().collect());
    }
}
