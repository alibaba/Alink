package com.alibaba.alink.operator.common.regression;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * The model data of isotonic regression.
 */
public class IsotonicRegressionModelData {
    public Params meta = new Params();
    public Double[] boundaries;
    public Double[] values;
}
