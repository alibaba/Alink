package com.alibaba.alink.operator.common.evaluation;

import java.io.Serializable;

/**
 * Evaluation curves for binary evaluation, including rocCurve, recallPrecisionCurve and liftChart.
 *
 * For rocCurve and recall PrecisionCurve, provide AUC and PRC(Area blow the curve).
 *
 * For rocCurve, provide KS(The maximum difference between TPR(y coordinate) and FPR(x coordinate)).
 */
public class EvaluationCurve implements Serializable {
	private EvaluationCurvePoint[] points;

	public EvaluationCurve(EvaluationCurvePoint[] points) {
		this.points = points;
	}

	public EvaluationCurvePoint[] getPoints() {
		return points;
	}

	/**
	 * Calculate the area below the curve, only used in rocCurve and recall PrecisionCurve, it's meaningless for LiftChart.
	 */
	double calcArea() {
		if (null == points) {
			return Double.NaN;
		}
		double s = 0.0;
		for (int i = 1; i < points.length; i++) {
			s += (points[i].getX() - points[i - 1].getX()) * (points[i].getY() + points[i - 1].getY()) / 2;
		}
		return s;
	}

	/**
	 * Calculate the maximum difference between y x coordinate, only used in rocCurve. It's meaningless for Recall-Precision Curve and LiftChart.
	 */
	double calcKs() {
		if (null == points) {
			return Double.NaN;
		}
		double s = 0.0;
		for (EvaluationCurvePoint point : points) {
			s = Math.max(s, Math.abs(point.getX() - point.getY()));
		}
		return s;
	}

	/**
	 * Return the X coordinates as one array and Y coordinate as another array. Union the two arrays as a two-dimension array.
	 */
	double[][] getXYArray() {
		double[] x = new double[points.length];
		double[] y = new double[points.length];
		for (int i = 0; i < points.length; i++) {
			x[i] = points[i].getX();
			y[i] = points[i].getY();
		}
		return new double[][] {x, y};
	}
}
