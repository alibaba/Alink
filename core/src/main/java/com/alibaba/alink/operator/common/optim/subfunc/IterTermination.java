package com.alibaba.alink.operator.common.optim.subfunc;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.linalg.DenseVector;

/**
 * Termination of iteration.
 *
 * if dir.f1[0] < 0.0 (which will be set when loss or gradNorm is small enough.), then iteration is terminated.
 */
public class IterTermination extends CompareCriterionFunction {

	private static final long serialVersionUID = -8181642524274951479L;

	@Override
	public boolean calc(ComContext context) {
		Tuple2 <DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
		return dir.f1[0] < 0.0;
	}
}

