package com.alibaba.alink.operator.common.feature.pca;

/**
 * pca calculation type.
 */
public enum PcaTypeEnum {
    /**
     * correlation
     */
    CORR,
    /**
     * sample variance
     */
    COV_SAMPLE,

    /**
     *  population variance
     */
    COVAR_POP
}
