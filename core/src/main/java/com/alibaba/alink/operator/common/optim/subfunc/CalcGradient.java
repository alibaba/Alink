package com.alibaba.alink.operator.common.optim.subfunc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

import java.util.List;

/**
 * Calculate gradient from labelVectors. labelVectors are stored in static memory. we use allReduce communication
 * pattern instead of map and broadcast. this class will be used by Lbfgs, Gd, Owlqn.
 */
public class CalcGradient extends ComputeFunction {

	private static final long serialVersionUID = 5489606217395507860L;
	/**
	 * object function class, it supply the functions to calc local gradient (or loss).
	 */
	private OptimObjFunc objFunc;

	@Override
	public void calc(ComContext context) {
		Iterable <Tuple3 <Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.trainData);

		// get iterative coefficient from static memory.
		Tuple2 <DenseVector, Double> state = context.getObj(OptimVariable.currentCoef);
		int size = state.f0.size();
		DenseVector coef = state.f0;
		if (objFunc == null) {
			objFunc = ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
		}
		Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
		// calculate local gradient
		Double weightSum = objFunc.calcGradient(labledVectors, coef, grad.f0);

		// prepare buffer vec for allReduce. the last element of vec is the weight Sum.
		double[] buffer = context.getObj(OptimVariable.gradAllReduce);
		if (buffer == null) {
			buffer = new double[size + 1];
			context.putObj(OptimVariable.gradAllReduce, buffer);
		}

		for (int i = 0; i < size; ++i) {
			buffer[i] = grad.f0.get(i) * weightSum;
		}

		/* the last element is the weight value */
		buffer[size] = weightSum;
	}
}
