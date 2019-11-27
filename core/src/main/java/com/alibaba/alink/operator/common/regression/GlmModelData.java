package com.alibaba.alink.operator.common.regression;


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
    public String familyName;

    /**
     * variance power of family.
     */
    public double variancePower;

    /**
     * link function name.
     */
    public String linkName;

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
