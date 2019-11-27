package com.alibaba.alink.operator.common.optim.subfunc;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Preallocate memory of skyk, which will be used by lbfgs and owlqn.
 *
 */
public class PreallocateSkyk extends ComputeFunction {
    private int numCorrections;

    public PreallocateSkyk(int numCorrections) {
        this.numCorrections = numCorrections;
    }

    /**
     * prepare hessian matrix of lbfgs method. we allocate memory fo sK, yK at first iteration step.
     *
     * @param context context of iteration.
     */
    @Override
    public void calc(ComContext context) {
        if (context.getStepNo() == 1) {
            Tuple2<DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
            int size = grad.f0.size();
            DenseVector[] sK = new DenseVector[numCorrections];
            DenseVector[] yK = new DenseVector[numCorrections];
            for (int i = 0; i < numCorrections; ++i) {
                sK[i] = new DenseVector(size);
                yK[i] = new DenseVector(size);
            }
            context.putObj(OptimVariable.sKyK, Tuple2.of(sK, yK));
        }
    }
}
