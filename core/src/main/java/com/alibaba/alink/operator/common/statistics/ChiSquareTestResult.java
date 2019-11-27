package com.alibaba.alink.operator.common.statistics;

/**
 * chi-square test result.
 */
public class ChiSquareTestResult {
    /**
     * comment: pearson chi-square independence test
     */
    private String comment;

    /**
     * freedom
     */
    private double df;
    /**
     * p value
     */
    private double p;
    /**
     * statistic value
     */
    private double value;


    public ChiSquareTestResult() {

    }
    /**
     * @param df:      degree freedom
     * @param p:       p value
     * @param value:   chi-square test value
     * @param comment: comment
     */
    public ChiSquareTestResult(double df,
                               double p,
                               double value,
                               String comment) {
        this.df = df;
        this.p = p;
        this.value = value;
        this.comment = comment;
    }


    public String getComment() {
        return comment;
    }

    public double getDf() {
        return df;
    }

    public double getP() {
        return p;
    }

    public double getValue() {
        return value;
    }
}