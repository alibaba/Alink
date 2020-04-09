package com.alibaba.alink.operator.common.regression;


import com.alibaba.alink.params.regression.GlmTrainParams;

/**
 * Glm model data.
 */
public class GlmModelData {

    /**
     * feature names.
     */
    public String[] featureColNames;

    /**
     * offset col name.
     */
    public String offsetColName;

    /**
     * weight col name.
     */
    public String weightColName;

    /**
     * label col name.
     */
    public String labelColName;


    /**
     * if fit intercept or not.
     */
    public boolean fitIntercept;

    /**
     * l2.
     */
    public double regParam;

    /**
     * iter of num.
     */
    public int numIter;

    /**
     * epsilon of loop.
     */
    public double epsilon;


    /**
     * family name.
     */
    public GlmTrainParams.Family familyName;

    /**
     * variance power of family.
     */
    public double variancePower;

    /**
     * link function name.
     */
    public GlmTrainParams.Link linkName;

    /**
     * power of link function.
     */
    public double linkPower;


    /**
     * coefficients of each features.
     */
    public double[] coefficients;

    /**
     * intercept.
     */
    public double intercept;

    /**
     * diag{1/(AT * A)}
     */
    public double[] diagInvAtWA;
}
