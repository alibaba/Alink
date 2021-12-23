package com.alibaba.alink.operator.common.timeseries;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.ArrayList;
import java.util.List;

/**
 * All matrix should be ordered as n x 1 matrix if it is a vector.Then the transpose matrix would be a 1 x n
 * matrix.
 * <p>
 * B: the quasi Hessian matrix
 * problem: the problem to be solved by BFGS
 * result: the estimated value of problem function like sum of square or likelihood
 */
public class BFGS {

	public BFGS() {
	}

	/**
	 * maxIter: maximum iteration
	 * threshold: the criteria to stop algorithm before the iteration reaches maxIter
	 *
	 * p: The search direction
	 * s: p times alpha
	 * y: gradient of X(i+1)-X(i)
	 * B: quasi Hessian matrix
	 * constrain: constrains on BFGS.
	 * 1(divide search direction by its norm),
	 * 2(divide loss function by its sample size),
	 * 3(accept weak converge)
	 * https://en.wikipedia.org/wiki/Broyden%E2%80%93Fletcher%E2%80%93Goldfarb%E2%80%93Shanno_algorithm
	 */
	public static AbstractGradientTarget solve(AbstractGradientTarget problem,
											   int maxIter,
											   double strongC,
											   double weakC,
											   int[] constrain,
											   int varianceIdx) {
		List <Integer> constrainList = new ArrayList <Integer>();
		for (int aConstrain : constrain) {
			constrainList.add(aConstrain);
		}

		boolean shortStep = constrainList.contains(1);
		boolean divideSample = constrainList.contains(2);
		boolean weak = constrainList.contains(3);
		boolean directWarn = false;
		boolean hessianWarn = false;

		int dim = problem.getInitCoef().numRows();
		DenseMatrix s;
		DenseMatrix y;
		DenseMatrix identity = DenseMatrix.eye(dim);
		DenseMatrix hessian = identity.clone();
		DenseMatrix coef = problem.getInitCoef();
		DenseMatrix newCoef = coef.clone();
		DenseMatrix g = ifMeanGradient(problem, coef, 0, divideSample);

		int i;
		for (i = 1; i < maxIter; i++) {
			DenseMatrix p = hessian.multiplies(g).scale(-1);
			if (shortStep) {
				for (int j = 0; j < p.numRows(); j++) {
					if (p.get(j, 0) > 1) {
						directWarn = true;
						break;
					}
				}
			}

			if (directWarn) {
				p = p.scale(1 / norm2(p));
			}

			double alpha = backtrackLineSearch(problem, coef, p, g, varianceIdx);
			s = p.scale(alpha);
			newCoef = coef.plus(s);

			if (varianceIdx >= 0 && newCoef.numRows() > varianceIdx) {
				if (newCoef.get(varianceIdx, 0) < 0) {
					break;
				}
			}

			DenseMatrix newGradient = ifMeanGradient(problem, newCoef, i, divideSample);

			y = newGradient.minus(g);
			double ys = y.transpose().multiplies(s).get(0, 0);
			DenseMatrix newH = identity.minus(s.multiplies(y.transpose()).scale(1 / ys)).multiplies(hessian)
				.multiplies(identity.minus(y.multiplies(s.transpose()).scale(1 / ys)))
				.plus(s.multiplies(s.transpose()).scale(1 / ys));

			for (int j = newH.numRows() - 1; j >= 0; j--) {
				if (Double.isNaN(newH.get(j, 0))) {
					hessianWarn = true;
					break;
				}
			}

			if (hessianWarn) {
				problem.setWarn("1");
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println(
						"Warn: Stop when Hessian Matrix contains NaN. Loss function CSS doesn't"
							+ " converge in gradient descent.");
				}
				break;
			}

			hessian = newH.clone();
			g = newGradient.clone();
			coef = newCoef.clone();

			if (norm2(newGradient) < strongC) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("Optimization terminated successfully.(Strong converge)");
				}
				break;
			}
			if (weak && (norm2(y) < weakC)) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("Optimization terminated successfully.(Weak converge)");
				}
				break;
			}


		}

		problem.setIter(i);

		problem.setFinalCoef(coef);
		problem.setH(hessian);
		problem.setMinValue(problem.f(coef));

		return problem;
	}

	/**
	 * coef: the coef matrix
	 * p: The search direction
	 * g: gradient of coef
	 * rho : The backtrack step between (0,1), usually 1/2
	 * c: parameter between 0 and 1, usually 10^{-4}
	 * https://en.wikipedia.org/wiki/Backtracking_line_search
	 */
	private static double backtrackLineSearch(AbstractGradientTarget problem,
											  DenseMatrix coef,
											  DenseMatrix p,
											  DenseMatrix g,
											  int varianceIdx) {

		//double alpha = Math.min(1.0, 1.01*2*(coef.minus(p)).times(g.inverse()).get(0,0));
		double alpha = 1;

		int maxIter = 10000;
		double rho = 0.5;
		double c = 0.00001;

		DenseMatrix d = coef.plus(p.scale(alpha));
		double variance = 0;
		if (varianceIdx >= 0 && d.numRows() > varianceIdx) {
			variance = d.get(varianceIdx, 0);
		}

		while (maxIter > 0 &&
			(problem.f(coef) - problem.f(coef.plus(p.scale(alpha)))
				< p.transpose().multiplies(g).scale(alpha * c).scale(-1).get(0, 0))
			&& variance >= 0) {
			alpha *= rho;
			maxIter--;
		}
		return alpha;
	}

	private static double norm2(DenseMatrix dm) {
		if (dm.numCols() == 1) {
			return new DenseVector(dm.getColumn(0)).normL2();
		} else {
			return dm.norm2();
		}
	}

	private static DenseMatrix ifMeanGradient(AbstractGradientTarget problem,
											  DenseMatrix coef,
											  int i,
											  boolean divideSample) {
		if (divideSample) {
			if (problem.getSampleSize() == -99) {
				return problem.gradient(coef, i).scale(1 / (double) problem.getX().numRows());
			} else {
				return problem.gradient(coef, i).scale(1 / (double) problem.getSampleSize());
			}
		} else {
			return problem.gradient(coef, i);
		}
	}

}
