package com.alibaba.alink.common.lazy;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.ExtractModelInfoBatchOp;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.TrainerLegacy;
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
 * Help create bounded instances of LazyEvaluation with regard to another object.
 * For a given object and a string key, always return a same lazy instance.
 */
public class LazyObjectsManager {
	/**
	 * records all operators called lazyPrint/lazyCollect before this `execute`.
	 * will clear after `execute` called.
	 */
	private final Map <BatchOperator <?>, LazyEvaluation <Pair <BatchOperator <?>, List <Row>>>> lazySinks
		= new LinkedHashMap <>();
	private final Map <EstimatorBase <?, ?>, LazyEvaluation <BatchOperator <?>>> lazyTrainOps = new HashMap <>();
	private final Map <EstimatorBase <?, ?>, LazyEvaluation <ModelBase <?>>> lazyModels = new HashMap <>();
	private final Map <TransformerBase <?>, LazyEvaluation <BatchOperator <?>>> lazyTransformResults = new HashMap
		<>();
	private final Map <BaseTuning <?, ?>, LazyEvaluation <Report>> lazyReports = new HashMap <>();

	/**
	 * `lazyOpsAfterLinked` works for `linkFrom` of {@link ExtractModelInfoBatchOp}.
	 * When A (of type ExtractModelInfoBatchOp) calls `linkFrom` B and B doesn't have outputTable yet,
	 * a lazyOpAfterLinked with type `LazyEvaluation<BatchOperator>` is generated for B.
	 * The `addValue` of lazyOpAfterLinked will be called just before `execute` with B itself.
	 * In its callback, A will do actual `linkFrom` with B.
	 *
	 * To make the usage simple, if A calls `lazyLinkFrom` from B, A cannot be linked or lazy-linked by others.
	 */
	private final Map <BatchOperator <?>, LazyEvaluation <BatchOperator <?>>> lazyOpsAfterLinked
		= new LinkedHashMap <>();

	public static LazyObjectsManager getLazyObjectsManager(HasMLEnvironmentId <?> hasMLEnvironmentId) {
		return MLEnvironmentFactory.get(hasMLEnvironmentId.getMLEnvironmentId()).getLazyObjectsManager();
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

	public LazyEvaluation <Pair <BatchOperator <?>, List <Row>>> genLazySink(BatchOperator <?> op) {
		checkLazyOpsAfterLinked();
		return genLazyObject(op, lazySinks);
	}

	public LazyEvaluation <BatchOperator <?>> genLazyTrainOp(Trainer <?, ?> trainer) {
		return genLazyObject(trainer, lazyTrainOps);
	}

	public LazyEvaluation <BatchOperator <?>> genLazyTrainOp(TrainerLegacy <?, ?> trainer) {
		return genLazyObject(trainer, lazyTrainOps);
	}

	public LazyEvaluation <ModelBase <?>> genLazyModel(Trainer <?, ?> trainer) {
		return genLazyObject(trainer, lazyModels);
	}

	public LazyEvaluation <ModelBase <?>> genLazyModel(TrainerLegacy <?, ?> trainer) {
		return genLazyObject(trainer, lazyModels);
	}

	public LazyEvaluation <BatchOperator <?>> genLazyTransformResult(TransformerBase <?> transformer) {
		return genLazyObject(transformer, lazyTransformResults);
	}

	public LazyEvaluation <BatchOperator <?>> genLazyOpAfterLinked(BatchOperator <?> op) {
		return genLazyObject(op, lazyOpsAfterLinked);
	}

	public LazyEvaluation <Report> genLazyReport(BaseTuning <?, ?> tuning) {
		return genLazyObject(tuning, lazyReports);
	}

	public Map <BatchOperator <?>, LazyEvaluation <Pair <BatchOperator <?>, List <Row>>>> getLazySinks() {
		return lazySinks;
	}

	public void clearVirtualSinks() {
		lazySinks.clear();
	}

	public void checkLazyOpsAfterLinked() {
		boolean flag = true;
		while (flag) {
			LinkedHashSet <Entry <BatchOperator <?>, LazyEvaluation <BatchOperator <?>>>> linkedEntries
				= lazyOpsAfterLinked.entrySet().stream()
				.filter(d -> !d.getKey().isNullOutputTable())
				.collect(Collectors.toCollection(LinkedHashSet::new));
			lazyOpsAfterLinked.entrySet().removeAll(linkedEntries);
			for (Entry <BatchOperator <?>, LazyEvaluation <BatchOperator <?>>> linkedEntry : linkedEntries) {
				BatchOperator <?> op = linkedEntry.getKey();
				LazyEvaluation <BatchOperator <?>> lazyLinkedOp = linkedEntry.getValue();
				lazyLinkedOp.addValue(op);
			}
			flag = !linkedEntries.isEmpty();
		}
	}

	public Map <BatchOperator <?>, LazyEvaluation <BatchOperator <?>>> getLazyOpsAfterLinked() {
		return lazyOpsAfterLinked;
	}

	public void clearLazyOpsAfterLinked() {
		lazyOpsAfterLinked.clear();
	}
}
