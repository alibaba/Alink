package com.alibaba.alink.operator.common.statistics;

import java.io.Serializable;

/**
 * chi-square test result.
 */
public class ChiSquareTestResult implements Serializable {
    private static final long serialVersionUID = 7324787640414737723L;
    /**
     * col name.
     */
    private String colName;

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
     * @param colName: colName
     */
    public ChiSquareTestResult(double df,
                               double p,
                               double value,
                               String colName) {
        this.df = df;
        this.p = p;
        this.value = value;
        this.colName = colName;
    }


    public String getColName() {
        return colName;
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

    public void setColName(String colName) {
        this.colName = colName;
    }
}