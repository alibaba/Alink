package com.alibaba.alink.operator.local.lazy;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.LocalMLEnvironment;
import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.common.lazy.LazyEvaluation;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.TransformerBase;
import com.alibaba.alink.pipeline.tuning.BaseTuning;
import com.alibaba.alink.pipeline.tuning.Report;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Help create bounded instances of LazyEvaluation with regard to another object. For a given object and a string key,
 * always return a same lazy instance.
 */
public class LocalLazyObjectsManager {
	/**
	 * records all operators called lazyPrint/lazyCollect before this `execute`. will clear after `execute` called.
	 */
	private final Map <LocalOperator <?>, LazyEvaluation <Pair <LocalOperator <?>, List <Row>>>> lazySinks
		= new LinkedHashMap <>();
	private final Map <EstimatorBase <?, ?>, LazyEvaluation <LocalOperator <?>>> lazyTrainOps = new HashMap <>();
	private final Map <EstimatorBase <?, ?>, LazyEvaluation <ModelBase <?>>> lazyModels = new HashMap <>();
	private final Map <TransformerBase <?>, LazyEvaluation <LocalOperator <?>>> lazyTransformResults = new HashMap
		<>();
	private final Map <BaseTuning <?, ?>, LazyEvaluation <Report>> lazyReports = new HashMap <>();

	/**
	 * `lazyOpsAfterLinked` works for `linkFrom` of {@link ExtractModelInfoBatchOp}. When A (of type
	 * ExtractModelInfoBatchOp) calls `linkFrom` B and B doesn't have outputTable yet, a lazyOpAfterLinked with type
	 * `LazyEvaluation<LocalOperator>` is generated for B. The `addValue` of lazyOpAfterLinked will be called just
	 * before `execute` with B itself. In its callback, A will do actual `linkFrom` with B.
	 * <p>
	 * To make the usage simple, if A calls `lazyLinkFrom` from B, A cannot be linked or lazy-linked by others.
	 */
	private final Map <LocalOperator <?>, LazyEvaluation <LocalOperator <?>>> lazyOpsAfterLinked
		= new LinkedHashMap <>();

	public static LocalLazyObjectsManager getLazyObjectsManager(LocalOperator <?> localOp) {
		return LocalMLEnvironment.getInstance().getLazyObjectsManager();
	}

	/**
	 * Generate a unique LazyEvaluation object for given obj and key.
	 *
	 * @param obj given obj
	 * @param <S> the type of LazyEvaluation value
	 * @return a LazyEvaluation object
	 */
	private <S, T> LazyEvaluation <S> genLazyObject(T obj, Map <T, LazyEvaluation <S>> lazyMap) {
		if (!lazyMap.containsKey(obj)) {
			lazyMap.put(obj, new LazyEvaluation <>());
		}
		return lazyMap.get(obj);
	}

	public LazyEvaluation <Pair <LocalOperator <?>, List <Row>>> genLazySink(LocalOperator <?> op) {
		checkLazyOpsAfterLinked();
		return genLazyObject(op, lazySinks);
	}

	public LazyEvaluation <LocalOperator <?>> genLazyTrainOp(Trainer <?, ?> trainer) {
		return genLazyObject(trainer, lazyTrainOps);
	}

	public LazyEvaluation <ModelBase <?>> genLazyModel(Trainer <?, ?> trainer) {
		return genLazyObject(trainer, lazyModels);
	}

	public LazyEvaluation <LocalOperator <?>> genLazyTransformResult(TransformerBase <?> transformer) {
		return genLazyObject(transformer, lazyTransformResults);
	}

	public LazyEvaluation <LocalOperator <?>> genLazyOpAfterLinked(LocalOperator <?> op) {
		return genLazyObject(op, lazyOpsAfterLinked);
	}

	public LazyEvaluation <Report> genLazyReport(BaseTuning <?, ?> tuning) {
		return genLazyObject(tuning, lazyReports);
	}

	public Map <LocalOperator <?>, LazyEvaluation <Pair <LocalOperator <?>, List <Row>>>> getLazySinks() {
		return lazySinks;
	}

	public void clearVirtualSinks() {
		lazySinks.clear();
	}

	public void checkLazyOpsAfterLinked() {
		boolean flag = true;
		while (flag) {
			LinkedHashSet <Entry <LocalOperator <?>, LazyEvaluation <LocalOperator <?>>>> linkedEntries
				= lazyOpsAfterLinked.entrySet().stream()
				.filter(d -> !d.getKey().isNullOutputTable())
				.collect(Collectors.toCollection(LinkedHashSet::new));
			lazyOpsAfterLinked.entrySet().removeAll(linkedEntries);
			for (Entry <LocalOperator <?>, LazyEvaluation <LocalOperator <?>>> linkedEntry : linkedEntries) {
				LocalOperator <?> op = linkedEntry.getKey();
				LazyEvaluation <LocalOperator <?>> lazyLinkedOp = linkedEntry.getValue();
				lazyLinkedOp.addValue(op);
			}
			flag = !linkedEntries.isEmpty();
		}
	}

	public Map <LocalOperator <?>, LazyEvaluation <LocalOperator <?>>> getLazyOpsAfterLinked() {
		return lazyOpsAfterLinked;
	}

	public void clearLazyOpsAfterLinked() {
		lazyOpsAfterLinked.clear();
	}
}
