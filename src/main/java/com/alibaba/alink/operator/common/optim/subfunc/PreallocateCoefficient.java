package com.alibaba.alink.operator.common.optim.subfunc;

import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Preallocate memory of coefficient and its corresponding loss value.
 * initial loss value is max_value
 *
 */
public class PreallocateCoefficient extends ComputeFunction {
    private String name;

    public PreallocateCoefficient(String name) {
        this.name = name;
    }

    @Override
    public void calc(ComContext context) {
        if (context.getStepNo() == 1) {
            List<DenseVector> coefs = context.getObj(OptimVariable.model);
            DenseVector coef = coefs.get(0);
            context.putObj(name, Tuple2.of(coef, Double.MAX_VALUE));
        }
    }
}
