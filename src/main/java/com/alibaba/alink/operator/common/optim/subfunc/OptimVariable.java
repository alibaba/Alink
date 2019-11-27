package com.alibaba.alink.operator.common.optim.subfunc;

/**
 * Some static variables defined here.
 *
 */
public class OptimVariable {
    public final static String dir = "direction";
    public final static String grad = "gradient";
    public final static String pseGrad = "pseGradient";
    public final static String trainData = "trainData";
    public final static String gradAllReduce = "gradAllReduce";
    public final static String lossAllReduce = "lossAllReduce";
    public final static String currentCoef = "currentCoef";
    public final static String minCoef = "minCoef";
    public final static String coef = "coef";
    public final static String model = "model";
    public final static String hessian = "hessian";
    public final static String sKyK = "sKyK";
    public final static String gradHessAllReduce = "gradHessAllReduce";
    public final static String lossCurve = "lossCurve";
    public final static String objFunc = "objFunc";
    public final static int numCorrections = 10;
    public final static double learningRate = 0.1;
}
