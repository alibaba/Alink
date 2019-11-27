package com.alibaba.alink.common.comqueue;

import java.io.Serializable;

/**
 * This function is executed at the end of each iteration to check whether to break iteration.
 */
public abstract class CompareCriterionFunction implements Serializable {

    /**
     * Check if it's ok to break iteration.
     *
     * @param context the ComContext
     * @return true to break.
     */
    public abstract boolean calc(ComContext context);

}
