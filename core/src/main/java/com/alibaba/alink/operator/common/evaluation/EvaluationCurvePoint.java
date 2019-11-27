package com.alibaba.alink.operator.common.evaluation;

import java.io.Serializable;

/**
 * Point definition for evaluation curves.
 *
 * x, y is the coordinate. p is the score threshold.
 */
public class EvaluationCurvePoint implements Serializable {
	private double x;
	private double y;
	private double p;

	public EvaluationCurvePoint(double x, double y, double p){
		this.x = x;
		this.y = y;
		this. p = p;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public double getP() {
		return p;
	}
}
