package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Termination of iteration.
 *
 * if dir.f1[0] < 0.0 (which will be set when loss or gradNorm is small enough.), then iteration is terminated.
 *
 */
public class IterTermination extends CompareCriterionFunction {

    @Override
    public boolean calc(ComContext context) {
        Tuple2<DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
        if (dir.f1[0] < 0.0) {
            return true;
        } else {
            return false;
        }
    }
}

