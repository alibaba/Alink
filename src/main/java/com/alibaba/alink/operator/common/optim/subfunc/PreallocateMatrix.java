package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.List;

/**
 * Preallocate memory of hessian matrix.
 *
 */
public class PreallocateMatrix extends ComputeFunction {
    private String name;
    private int maxDim;

    public PreallocateMatrix(String name, int maxDim) {
        this.name = name;
        this.maxDim = maxDim;
    }

    @Override
    public void calc(ComContext context) {
        if (context.getStepNo() == 1) {
            List<DenseVector> coefs = context.getObj(OptimVariable.model);
            DenseVector coef = coefs.get(0);
            if (coef.size() > maxDim) {
                throw new RuntimeException("matrix vectorSize is larger than " + maxDim);
            }
            DenseMatrix mat = new DenseMatrix(coef.size(), coef.size());
            context.putObj(name, mat);
        }
    }
}
