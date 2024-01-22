package com.alibaba.alink.operator.local.lazy;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.source.BaseSourceLocalOp;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class ExtractModelInfoLocalOp<S, T extends ExtractModelInfoLocalOp <S, T>> extends LocalOperator <T> {
	private static final long serialVersionUID = 8426490988920758149L;

	public ExtractModelInfoLocalOp(Params params) {
		super(params);
	}

	/**
	 * This modified version of `linkFrom` will first check `isNullOutputTable` of op. If false, then immediately call
	 * `setOutputTable`. Otherwise,`setOutputTable` is called in the callback, and will be trigger just before
	 * `execute`.
	 *
	 * @param inputs the linked inputs
	 * @return this
	 */
	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> op = checkAndGetFirst(inputs);
		if (op.isNullOutputTable() && !(op instanceof BaseSourceLocalOp)) {
			LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager(op);
			LazyEvaluation <LocalOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(op);
			lazyOpAfterLinked.addCallback(d -> setOutputTable(d.getOutputTable()));
		} else {
			setOutputTable(op.getOutputTable());
		}
		//noinspection unchecked
	}

	protected abstract S createModelInfo(List <Row> rows);

	protected LocalOperator <?> processModel() {
		return this;
	}

	final public T lazyPrintModelInfo(String title) {
		//noinspection unchecked
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
		Consumer <LocalOperator <?>> consumer =
			op -> ((ExtractModelInfoLocalOp <?, ?>) op)
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
			LocalLazyObjectsManager lazyObjectsManager = LocalLazyObjectsManager.getLazyObjectsManager(this);
			LazyEvaluation <LocalOperator <?>> lazyOpAfterLinked = lazyObjectsManager.genLazyOpAfterLinked(this);
			lazyOpAfterLinked.addCallback(consumer);
		} else {
			consumer.accept(this);
		}

		//noinspection unchecked
		return ((T) this);
	}

	@SuppressWarnings("unchecked")
	final public T lazyCollectModelInfo(Consumer <S>... callbacks) {
		return lazyCollectModelInfo(Arrays.asList(callbacks));
	}

	final public S collectModelInfo() {
		return createModelInfo(this.processModel().collect());
	}
}
