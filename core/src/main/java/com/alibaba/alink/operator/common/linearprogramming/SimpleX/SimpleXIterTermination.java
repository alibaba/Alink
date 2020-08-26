package com.alibaba.alink.operator.common.linearprogramming.SimpleX;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.operator.batch.linearprogramming.SimpleXBatchOp;

/**
 * Judge whether algorithm completes. Completed when no pivot col nor row.
 */
public class SimpleXIterTermination extends CompareCriterionFunction {
    @Override
    public boolean calc(ComContext context) {
        double[] pivotRowList = context.getObj(SimpleXBatchOp.PIVOT_ROW_VALUE);
        if (pivotRowList[0] < 0) {
            System.out.println("completed");
            return true;
        }
        return false;
    }
}
