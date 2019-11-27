package com.alibaba.alink.operator.common.optim;

import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * The factory of optimizer.
 */
public class OptimizerFactory {
	public static Optimizer create(
		DataSet<OptimObjFunc> objFunc,
		DataSet <Tuple3 <Double, Double, Vector>> trainData,
		DataSet <Integer> coefDim,
		Params params,
		OptimMethod method) {
		switch (method) {
			case SGD:
				return new Sgd(objFunc, trainData, coefDim, params);
			case NEWTON:
				return new Newton(objFunc, trainData, coefDim, params);
			case GD:
				return new Gd(objFunc, trainData, coefDim, params);
			case LBFGS:
				return new Lbfgs(objFunc, trainData, coefDim, params);
			case OWLQN:
				return new Owlqn(objFunc, trainData, coefDim, params);
			default:
				throw new UnsupportedOperationException("Optimization method not found.");
		}
	}
}
