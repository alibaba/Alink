package com.alibaba.alink.common.lazy;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.BaseSourceBatchOp;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class ExtractModelInfoBatchOp<S, T extends ExtractModelInfoBatchOp <S, T>> extends BatchOperator <T> {
	private static final long serialVersionUID = 8426490988920758149L;

	public ExtractModelInfoBatchOp(Params params) {
		super(params);
	}

	/**
	 * This modified version of `linkFrom` will first check `isNullOutputTable` of op.
	 * If false, then immediately call `setOutputTable`.
	 * Otherwise,`setOutputTable` is called in the callback, and will be trigger just before `execute`.
	 *
	 * @param inputs the linked inputs
	 * @return
	 */
	@Override
	final public T linkFrom(BatchOperator <?>... inputs) {
		BatchOperator <?> op = checkAndGetFirst(inputs);
		if (op.isNullOutputTable() && !(op instanceof BaseSourceBatchOp)) {
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(op);
			LazyEvaluation <BatchOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(op);
			lazyOpAfterLinked.addCallback(d -> setOutputTable(d.getOutputTable()));
		} else {
			setOutputTable(op.getOutputTable());
		}
		return (T) this;
	}

	protected abstract S createModelInfo(List <Row> rows);

	protected BatchOperator <?> processModel() {
		return this;
	}

	final public T lazyPrintModelInfo(String title) {
		return lazyCollectModelInfo(d -> {
			if (null != title) {
				System.out.println(title);
			}
			System.out.println(d);
		});
	}

	final public T lazyPrintModelInfo() {
		return lazyPrintModelInfo(null);
	}

	final public T lazyCollectModelInfo(List <Consumer <S>> callbacks) {

		Consumer <BatchOperator <?>> consumer =
			op -> ((ExtractModelInfoBatchOp <?, ?>) op)
				.processModel()
				.lazyCollect(
					d -> {
						S summary = createModelInfo(d);
						for (Consumer <S> callback : callbacks) {
							callback.accept(summary);
						}
					}
				);

		if (this.isNullOutputTable()) {
			LazyObjectsManager lazyObjectsManager = LazyObjectsManager.getLazyObjectsManager(this);
			LazyEvaluation <BatchOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(this);
			lazyOpAfterLinked.addCallback(consumer);
		} else {
			consumer.accept(this);
		}

		return ((T) this);
	}

	final public T lazyCollectModelInfo(Consumer <S>... callbacks) {
		return lazyCollectModelInfo(Arrays.asList(callbacks));
	}

	final public S collectModelInfo() {
		return createModelInfo(this.processModel().collect());
	}
}
