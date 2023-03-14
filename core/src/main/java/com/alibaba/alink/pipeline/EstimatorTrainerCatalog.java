package com.alibaba.alink.pipeline;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.local.LocalOperator;
import org.reflections.Reflections;

import java.util.HashMap;

public class EstimatorTrainerCatalog {

	private final static HashMap <String, Class <? extends BatchOperator <?>>> mapBatch = new HashMap <>();
	private final static HashMap <String, Class <? extends LocalOperator <?>>> mapLocal = new HashMap <>();

	public static void registerBatchTrainer(String estimatorName, Class <? extends BatchOperator <?>> clazz) {
		mapBatch.put(estimatorName, clazz);
	}

	public static void registerLocalTrainer(String estimatorName, Class <? extends LocalOperator <?>> clazz) {
		mapLocal.put(estimatorName, clazz);
	}

	public static Class <? extends BatchOperator <?>> lookupBatchTrainer(String estimatorName) {
		return mapBatch.get(estimatorName);
	}

	public static Class <? extends LocalOperator <?>> lookupLocalTrainer(String estimatorName) {
		return mapLocal.get(estimatorName);
	}

	static {
		for (String prefix : new String[] {"com.alibaba.alink", "org.alinklab"}) {
			Reflections reflections = new Reflections(prefix);
			for (Class <?> clazz : reflections.getTypesAnnotatedWith(EstimatorTrainerAnnotation.class)) {
				try {
					String estimatorName = clazz.getAnnotation(EstimatorTrainerAnnotation.class).estimatorName();
					if (BatchOperator.class.isAssignableFrom(clazz)) {
						registerBatchTrainer(estimatorName, (Class <? extends BatchOperator <?>>) clazz);
					} else if (LocalOperator.class.isAssignableFrom(clazz)) {
						registerLocalTrainer(estimatorName, (Class <? extends LocalOperator <?>>) clazz);
					}
				} catch (Throwable th) {
				}
			}
		}
	}

}
