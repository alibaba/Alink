package com.alibaba.alink.operator.common.optim.subfunc;

import java.util.Arrays;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;

/**
 * Preallocate memory for loss value of every iteration.
 *
 */
public class PreallocateLossCurve extends ComputeFunction {
    private String name;
    private int maxIter;

    /**
     *
     * @param name     name for this memory.
     * @param lossNum  num of loss values.
     */
    public PreallocateLossCurve(String name, int lossNum) {
        this.name = name;
        this.maxIter = lossNum;
    }

    @Override
    public void calc(ComContext context) {
        if (context.getStepNo() == 1) {
            double[] lossCurve = new double[maxIter];
            Arrays.fill(lossCurve, Double.POSITIVE_INFINITY);
            context.putObj(name, lossCurve);
        }
    }
}
