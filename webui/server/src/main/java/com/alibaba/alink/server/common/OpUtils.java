package com.alibaba.alink.server.common;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public class OpUtils {

	@SuppressWarnings("unchecked")
	public static AlgoOperator <?> makeOp(String className, Params params, List <AlgoOperator <?>> ops)
		throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException,
		IllegalAccessException {
		Class <?> clz = Class.forName(className);
		if (BatchOperator.class.isAssignableFrom(clz)) {
			return makeBatchOp((Class <? extends BatchOperator <?>>) clz, params, ops);
		} else if (StreamOperator.class.isAssignableFrom(clz)) {
			return makeStreamOp((Class <? extends StreamOperator <?>>) clz, params, ops);
		} else {
			throw new UnsupportedOperationException(
				String.format("%s is neither a BatchOperator nor a StreamOperator!", className));
		}
	}

	public static BatchOperator <?> makeBatchOp(Class <? extends BatchOperator <?>> clz,
												Params params, List <AlgoOperator <?>> ops)
		throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		Constructor <?> constructor = clz.getConstructor(Params.class);
		BatchOperator <?> op = (BatchOperator <?>) constructor.newInstance(params);
		if (ops.size() > 0) {
			List <BatchOperator <?>> batchOps = ops.stream().map(
				d -> (BatchOperator <?>) (d)
			).collect(Collectors.toList());
			op.linkFrom(batchOps);
		}
		return op;
	}

	static boolean hasModelOpConstructor(Class <? extends StreamOperator <?>> clz) {
		try {
			Constructor <?> constructor = clz.getConstructor(BatchOperator.class, Params.class);
			return true;
		} catch (NoSuchMethodException e) {
			return false;
		}
	}

	public static StreamOperator <?> makeStreamOp(Class <? extends StreamOperator <?>> clz,
												  Params params, List <AlgoOperator <?>> ops)
		throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		boolean hasModelOpConstructor;
		Constructor <?> constructor;
		try {
			constructor = clz.getConstructor(BatchOperator.class, Params.class);
			hasModelOpConstructor = true;
		} catch (NoSuchMethodException e) {
			constructor = clz.getConstructor(Params.class);
			hasModelOpConstructor = false;
		}

		StreamOperator <?> op;
		if (hasModelOpConstructor) {
			op = (StreamOperator <?>) constructor.newInstance(ops.get(0), params);
			ops = ops.subList(1, ops.size());
		} else {
			op = (StreamOperator <?>) constructor.newInstance(params);
		}
		if (ops.size() > 0) {
			List <StreamOperator <?>> streamOps = ops.stream()
				.map(d -> (StreamOperator <?>) d)
				.collect(Collectors.toList());
			op.linkFrom(streamOps);
		}
		return op;
	}
}
