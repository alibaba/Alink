package com.alibaba.alink.operator.common.linearprogramming.InteriorPoint;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.operator.batch.linearprogramming.InteriorPointBatchOp;

/**
 * Judge the termination condition of interior point method.
 */
public class InteriorPointIterTermination extends CompareCriterionFunction {
    @Override
    public boolean calc(ComContext context) {
        boolean go = context.getObj(InteriorPointBatchOp.CONDITION_GO);
        return go;
    }
}