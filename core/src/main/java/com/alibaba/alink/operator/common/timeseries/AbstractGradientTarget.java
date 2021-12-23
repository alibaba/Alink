package com.alibaba.alink.operator.common.timeseries;

import com.alibaba.alink.common.linalg.DenseMatrix;

import java.util.ArrayList;

/**
 * Coef: coefficients to be solved
 * X: the feature matrix of problem. Each row is a sample with n features in columns.
 * Y: the true label or value matrix of X
 * H: the Hessian matrix or quasi Hessian matrix
 * finalCoef: the final estimated coefficients.
 * minValue: the estimated value of problem function like sum of square or likelihood
 * residual: estimated residual using finalCoef.
 * warn: warning in conducting gradient descent.
 * sampleSize: the number of data used for averaging gradient. Mainly for the constrain 2 in BFGS.
 * Could be any number depending on the model. If it is not set, ifMeanGradient uses row dimension of X
 * gradient: return a n x 1 DenseMatrix of gradients of parameters at "coef",ordered as sARCoef, maCoef, intercept.
 * f: return conditional sum of square, given "coef".
 */
abstract public class AbstractGradientTarget {
	protected DenseMatrix initCoef;
	protected DenseMatrix x;
	protected DenseMatrix y;
	protected double minValue;
	protected double[] residual;
	protected DenseMatrix finalCoef;
	protected DenseMatrix hessian;
	protected ArrayList <String> warn;
	protected int iter;
	protected int sampleSize;

	public AbstractGradientTarget() {
		this.minValue = Double.MAX_VALUE;
		this.iter = -99;
		this.sampleSize = -99;
	}

	public AbstractGradientTarget(DenseMatrix initCoef, DenseMatrix x, DenseMatrix y) {
		this.initCoef = initCoef;
		this.x = x;
		this.y = y;
	}

	public DenseMatrix getInitCoef() {
		return this.initCoef.clone();
	}

	public DenseMatrix getY() {
		return this.y.clone();
	}

	public void setY(DenseMatrix y) {
		this.y = y.clone();
	}

	public DenseMatrix getX() {
		return this.x.clone();
	}

	public void setX(DenseMatrix x) {
		this.x = x.clone();
	}

	public double getMinValue() {
		return this.minValue;
	}

	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}

	public double[] getResidual() {
		return residual.clone();
	}

	public void setResidual(double[] residual) {
		this.residual = residual;
	}

	public int getIter() {
		return this.iter;
	}

	public void setIter(int i) {
		this.iter = i;
	}

	public DenseMatrix getH() {
		return this.hessian.clone();
	}

	public void setH(DenseMatrix hessian) {
		this.hessian = hessian;
	}

	public DenseMatrix getFinalCoef() {
		return this.finalCoef.clone();
	}

	public void setFinalCoef(DenseMatrix finalCoef) {
		this.finalCoef = finalCoef.clone();
	}

	public ArrayList <String> getWarn() {
		return this.warn == null ? null : new ArrayList <String>(this.warn);
	}

	public void setWarn(String warn) {
		if (this.warn == null) {
			this.warn = new ArrayList <String>();
		}
		this.warn.add(warn);
	}

	public int getSampleSize() {
		return this.sampleSize;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	public abstract DenseMatrix gradient(DenseMatrix coef, int iter);

	public abstract double f(DenseMatrix coef);

}
