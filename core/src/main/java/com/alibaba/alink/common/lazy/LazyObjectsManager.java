package com.alibaba.alink.common.lazy;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.shared.HasMLEnvironmentId;
import com.alibaba.alink.pipeline.EstimatorBase;
import com.alibaba.alink.pipeline.ModelBase;
import com.alibaba.alink.pipeline.Trainer;
import com.alibaba.alink.pipeline.TransformerBase;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Help create bounded instances of LazyEvaluation with regard to another object.
 * For a given object and a string key, always return a same lazy instance.
 */
public class LazyObjectsManager {
    /**
     * records all operators called lazyPrint/lazyCollect before this `execute`.
     * will clear after `execute` called.
     */
    private final Map<BatchOperator<?>, LazyEvaluation<Pair<BatchOperator<?>, List<Row>>>> lazySinks = new LinkedHashMap<>();
    private final Map<EstimatorBase<?, ?>, LazyEvaluation<BatchOperator<?>>> lazyTrainOps = new HashMap<>();
    private final Map<EstimatorBase<?, ?>, LazyEvaluation<ModelBase<?>>> lazyModels = new HashMap<>();
    private final Map<TransformerBase<?>, LazyEvaluation<BatchOperator<?>>> lazyTransformResults = new HashMap<>();

    public static LazyObjectsManager getLazyObjectsManager(HasMLEnvironmentId<?> hasMLEnvironmentId) {
        return MLEnvironmentFactory.get(hasMLEnvironmentId.getMLEnvironmentId()).getLazyObjectsManager();
    }

    /**
     * Generate a unique LazyEvaluation object for given obj and key.
     *
     * @param obj given obj
     * @param <S> the type of LazyEvaluation value
     * @return a LazyEvaluation object
     */
    private <S, T> LazyEvaluation<S> genLazyObject(T obj, Map<T, LazyEvaluation<S>> lazyMap) {
        if (!lazyMap.containsKey(obj)) {
            lazyMap.put(obj, new LazyEvaluation<>());
        }
        return lazyMap.get(obj);
    }

    public LazyEvaluation<Pair<BatchOperator<?>, List<Row>>> genLazySink(BatchOperator<?> op) {
        return genLazyObject(op, lazySinks);
    }

    public LazyEvaluation<BatchOperator<?>> genLazyTrainOp(Trainer<?, ?> trainer) {
        return genLazyObject(trainer, lazyTrainOps);
    }

    public LazyEvaluation<ModelBase<?>> genLazyModel(Trainer<?, ?> trainer) {
        return genLazyObject(trainer, lazyModels);
    }

    public LazyEvaluation<BatchOperator<?>> genLazyTransformResult(TransformerBase<?> transformer) {
        return genLazyObject(transformer, lazyTransformResults);
    }

    public Map<BatchOperator<?>, LazyEvaluation<Pair<BatchOperator<?>, List<Row>>>> getLazySinks() {
        return lazySinks;
    }

    public void clearVirtualSinks() {
        lazySinks.clear();
    }
}
