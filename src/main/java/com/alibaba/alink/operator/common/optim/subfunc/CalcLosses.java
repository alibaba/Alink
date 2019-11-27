package com.alibaba.alink.operator.common.optim.subfunc;

import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.OptimMethod;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Calculate losses from labelVectors. labelVectors are stored in static memory. we use allReduce communication
 * pattern instead of map and broadcast. this class will be used by Lbfgs, Gd, Owlqn.
 *
 */
public class CalcLosses extends ComputeFunction {

    /**
     * object function class, it supply the functions to calc gradient (or loss) locality.
     */
    private OptimObjFunc objFunc;
    private OptimMethod method;
    private int numSearchStep;

    /**
     * calc losses.
     *
     * @param method        optimization method.
     * @param numSearchStep num search step in line search.
     */
    public CalcLosses(OptimMethod method, int numSearchStep) {
        this.method = method;
        this.numSearchStep = numSearchStep;
    }

    @Override
    public void calc(ComContext context) {
        Iterable<Tuple3<Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.trainData);
        Tuple2<DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
        Tuple2<DenseVector, Double> coef = context.getObj(OptimVariable.currentCoef);
        if (objFunc == null) {
            objFunc = ((List<OptimObjFunc>)context.getObj(OptimVariable.objFunc)).get(0);
        }
        /**
         *  calculate losses of current coefficient.
         *  if optimizer is owlqn, constraint search will used, else without constraint.
         */
        Double beta = dir.f1[1] / numSearchStep;
        double[] vec = method.equals(OptimMethod.OWLQN) ?
            objFunc.constraintCalcSearchValues(labledVectors, coef.f0, dir.f0, beta, numSearchStep)
            : objFunc.calcSearchValues(labledVectors, coef.f0, dir.f0, beta, numSearchStep);

        // prepare buffer vec for allReduce.
        double[] buffer = context.getObj(OptimVariable.lossAllReduce);
        if (buffer == null) {
            buffer = vec.clone();
            context.putObj(OptimVariable.lossAllReduce, buffer);
        } else {
            System.arraycopy(vec, 0, buffer, 0, vec.length);
        }
    }
}
