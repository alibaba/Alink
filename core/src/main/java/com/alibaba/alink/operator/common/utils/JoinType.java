package com.alibaba.alink.operator.common.utils;

import java.io.Serializable;

public enum JoinType implements Serializable {
    /**
     * Cross join
     */
    CROSS,
    /**
     * Left outer join
     */
    LEFTOUTER
}
