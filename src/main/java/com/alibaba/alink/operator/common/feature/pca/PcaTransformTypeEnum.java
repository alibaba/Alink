package com.alibaba.alink.operator.common.feature.pca;

/**
 * pca transform type.
 */
public enum PcaTransformTypeEnum {
    /**
     *  data * model
     */
    SIMPLE,

    /**
     * (data - mean) * model
     */
    SUBMEAN,

    /**
     * (data - mean) / stdVar * model
     */
    NORMALIZATION
}
