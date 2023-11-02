package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

import java.util.Arrays;

public class SqpPai {
	public static boolean[] getActiveSet(DenseMatrix inequalityConstraint, DenseVector inequalityItem,
										 DenseVector dir, int dim) {
		int inequalNum = inequalityItem.size();
		boolean[] activeSet = new boolean[inequalNum];
		Arrays.fill(activeSet, false);
		for (int i = 0; i < inequalNum; i++) {
			double sum = 0;
			double[] row = inequalityConstraint.getRow(i);
			for (int j = 0; j < dim; j++) {
				sum += row[j] * dir.get(j);
			}
			if (Math.abs(sum - inequalityItem.get(i)) <= 1e-7) {
				activeSet[i] = true;
			} else {
				if (!(sum < inequalityItem.get(i))) {
					activeSet[i] = true;
				}
			}
		}
		return activeSet;
	}

	//<alpha, active>
	//todo check dir and weight, which is qp_dir and dir.
	private static Tuple2 <Double, Integer> searchActiveSet(DenseMatrix inequalityConstraint,
															DenseVector inequalityItem,
															DenseVector dir, DenseVector qpDir, boolean[] activeSet,
															int dim) {
		double alpha = 1.;
		int inequalNum = inequalityItem.size();
		int active = -1;
		for (int i = 0; i < inequalNum; i++) {
			if (activeSet[i]) {
				continue;
			}
			double ax = 0;
			double ap = 0;
			double p = 0;
			for (int j = 0; j < dim; j++) {
				double val = inequalityConstraint.get(i, j);
				ax -= val * dir.get(j);
				ap -= val * qpDir.get(j);
			}
			if (ap >= 0) {
				continue;
			}
			p = (-inequalityItem.get(i) - ax) / ap;
			if (p < alpha) {
				alpha = p;
				active = i;//满足条件的话就设置i是满足要求，可以active的。
			}
		}
		return Tuple2.of(alpha, active);
	}

	private static double[][] enableActiveSet(DenseMatrix hessian, int dim, boolean[] activeSet,
											  DenseMatrix equalityConstraint, DenseVector equalityItem,
											  DenseMatrix inequalityConstraint, DenseVector inequalityItem) {
		int equalNum = equalityItem.size();
		int inequalNum = inequalityItem.size();
		int kktSize = calcKktSize(dim, equalityItem, activeSet);
		double[][] h = new double[kktSize][kktSize];
		double[][] hessianArray = hessian.getArrayCopy2D();
		SqpUtil.fillMatrix(h, 0, 0, hessianArray);
		for (int i = 0; i < equalNum; i++) {
			int row = i + dim;
			for (int j = 0; j < dim; j++) {
				h[row][j] = equalityConstraint.get(i, j);
				h[j][row] = equalityConstraint.get(i, j);
			}
		}
		int asCnt = 0;
		for (int i = 0; i < inequalNum; i++) {
			if (activeSet[i]) {
				int row = equalNum + dim + asCnt;
				for (int j = 0; j < dim; j++) {
					h[row][j] = inequalityConstraint.get(i, j);
					h[j][row] = inequalityConstraint.get(i, j);
				}
				asCnt++;
			}
		}
		return h;
	}

	//get max activeSet
	private static Tuple2 <boolean[], Boolean> checkLambda(DenseMatrix equalityConstraint,
														   DenseMatrix inequalityConstraint, int dim,
														   boolean[] activeSet, DenseVector qpDir) {
		int inequalNum = inequalityConstraint.numRows();
		int equalNum = equalityConstraint.numRows();
		int lagrangeId = 0;
		double maxLam = -Double.MAX_VALUE;
		int maxId = -1;
		for (int i = 0; i < inequalNum; i++) {
			if (activeSet[i]) {
				double lambda = qpDir.get(dim + equalNum + lagrangeId);
				if (lambda > maxLam) {
					maxId = i;
					maxLam = lambda;
				}
				lagrangeId++;
			}
		}
		if (maxId > 0 && maxLam > 0) {
			activeSet[maxId] = false;
			return Tuple2.of(activeSet, true);
		}
		return Tuple2.of(activeSet, false);
	}

	//todo 注意一下，pai上有dim，也就是只乘前面的数据
	// input: <hessian, dir, grad, dim>
	//loss = 1/2 * hessian * dir *  dir + dir * grad
	private static double calculateQpLoss(DenseMatrix h, DenseVector p, DenseVector g, int dim) {
		double loss;
		DenseVector tmp = new DenseVector(dim);
		matDotVec(h, p, tmp, dim);
		loss = dot(tmp, p, dim);
		return loss * 0.5 + dot(p, g, dim);
	}

	//return <qpGrad, normL2(qpGrad)
	private static Tuple2 <DenseVector, Double> solveQuadProblem(DenseMatrix equalityConstraint,
																 DenseVector equalityItem,
																 DenseVector qpDir,
																 DenseMatrix inequalityConstraint,
																 DenseVector inequalityItem,
																 DenseVector dir, boolean[] activeSet,
																 DenseMatrix hessian,
																 DenseVector grad, DenseVector weight) {
		int dim = weight.size();
		//dim + count(equality) + count(activeSet=true,也就是lambda<0)
		int kktSize = calcKktSize(dim, equalityItem, activeSet);
		DenseVector gpGrad = new DenseVector(kktSize);
		// gpGrad = hessian * dir
		matDotVec(hessian, dir, gpGrad, dim);
		// gpGrad = gpGrad + grad = grad + hessian * dir
		vecAddVec(grad, gpGrad, dim);
		// h 是对称矩阵，按照activeSet筛选的规则矩阵(投影), 前面是hessian，后面是规则矩阵
		DenseMatrix h = new DenseMatrix(
			enableActiveSet(hessian, dim, activeSet,
				equalityConstraint, equalityItem, inequalityConstraint, inequalityItem));
		double norm = 1 / gpGrad.normL1();
		// h = h / |gpGrad|
		h.scaleEqual(norm);
		// gpGrad = gpGrad / |gpGrad|
		gpGrad.scaleEqual(norm);
		//gpDir = H^-1 * gbGrad = H^-1 * (grad + hessian * dir) / |gpGrad|
		try {
			DenseMatrix ginvH = h.inverse();
			qpDir = ginvH.multiplies(gpGrad);
		} catch (Exception e) {
			return Tuple2.of(qpDir, -1.);
		}
		//sum是 normL2(qpDir)/dim
		double sum = 0;
		for (int i = 0; i < dim; i++) {
			sum += Math.pow(qpDir.get(i), 2);
		}
		sum = 1. * Math.sqrt(sum) / dim;
		return Tuple2.of(qpDir, sum);
	}

	//the main run func.
	// dir = dir + alpha * gpDir
	// activeSet will modify.
	private static boolean solveActiveSetProblem(DenseMatrix equalityConstraint, DenseVector equalityItem,
												 DenseMatrix inequalityConstraint, DenseVector inequalityItem,
												 int dim, DenseVector dir, boolean[] activeSet, DenseMatrix hessian,
												 DenseVector grad, DenseVector weight) {
		int iterTime = inequalityItem.size();
		if (iterTime == 0) {
			iterTime = 1;
		}
		double loss = 0;
		double lastLoss = 0;
		//dim + equalCount + count(activeSet=true)
		int kktSize = calcKktSize(dim, equalityItem, activeSet);
		DenseVector qpDir = new DenseVector(kktSize);
		for (int i = 0; i < iterTime; i++) {
			//<qpGrad, sum>
			Tuple2 <DenseVector, Double> items = solveQuadProblem(equalityConstraint, equalityItem, qpDir,
				inequalityConstraint, inequalityItem,
				dir, activeSet, hessian, grad, weight);
			double p = items.f1;
			qpDir = items.f0;
			if (p < 0) {
				return false;
			}
			if (p < 1e-6) {
				Tuple2 <boolean[], Boolean> res = checkLambda(equalityConstraint, inequalityConstraint, dim,
					activeSet, qpDir);
				activeSet = res.f0;
				if (!res.f1) {
					break;
				}
				continue;
			}
			Tuple2 <Double, Integer> items2 = searchActiveSet(inequalityConstraint, inequalityItem, dir, qpDir,
				activeSet, dim);
			double alpha = items2.f0;
			int activeConst = items2.f1;
			if (activeConst >= 0) {
				activeSet[activeConst] = true;
			}
			//初始的dir是全0的，在存在约束条件的时候，会迭代，将dir累加上去。
			for (int j = 0; j < dim; j++) {
				dir.add(j, alpha * qpDir.get(j));
			}
			loss = calculateQpLoss(hessian, dir, grad, dim);
			if (lastLoss != 0) {
				double cond = (lastLoss - loss) / Math.abs(loss);
				if (cond < 1e-6) {
					Tuple2 <boolean[], Boolean> res = checkLambda(equalityConstraint, inequalityConstraint, dim,
						activeSet, qpDir);
					activeSet = res.f0;
					if (!res.f1) {
						break;
					}
				}
			}
			lastLoss = loss;
		}
		return true;
	}

	// res = matrix * vector
	private static void matDotVec(DenseMatrix matrix, DenseVector vector, DenseVector res, int dim) {
		for (int i = 0; i < dim; i++) {
			res.set(i, 0);
			double[] row = matrix.getRow(i);
			for (int j = 0; j < dim; j++) {
				res.add(i, row[j] * vector.get(j));
			}
		}
	}

	// dv2 = dv2 + dv1
	public static void vecAddVec(DenseVector dv1, DenseVector dv2, int dim) {
		for (int i = 0; i < dim; i++) {
			dv2.add(i, dv1.get(i));
		}
	}

	private static double dot(DenseVector d1, DenseVector d2, int dim) {
		double res = 0;
		for (int i = 0; i < dim; i++) {
			res += d1.get(i) * d2.get(i);
		}
		return res;
	}

	private static int countInequalNum(boolean[] activeSet) {
		int num = 0;
		for (boolean b : activeSet) {
			if (b) {
				num++;
			}
		}
		return num;
	}

	private static int calcKktSize(int dim, DenseVector equalityItem, boolean[] activeSet) {
		return dim + equalityItem.size() + countInequalNum(activeSet);
	}

	//<new dir, grad, hession>
	public static Tuple3 <DenseVector, DenseVector, DenseMatrix>
	calcDir(double retryTime, int dim, ConstraintObjFunc sqpObjFunc, DenseVector dir,
			DenseVector weight, DenseMatrix hessian, DenseVector grad,
			double l2Weight, double minL2Weight, boolean hasIntercept, boolean[] activeSet) {
		DenseMatrix equalityConstraint = sqpObjFunc.equalityConstraint;
		DenseMatrix inequalityConstraint = sqpObjFunc.inequalityConstraint;
		DenseVector equalityItem = sqpObjFunc.equalityItem;
		DenseVector inequalityItem = sqpObjFunc.inequalityItem;
		for (int i = 0; i < retryTime; i++) {
			boolean pass = solveActiveSetProblem(equalityConstraint, equalityItem, inequalityConstraint,
				inequalityItem,
				dim, dir, activeSet, hessian, grad, weight);
			if (pass) {
				break;
			}
			//update gradient and hessian
			int begin = 0;
			double l2 = l2Weight + minL2Weight;
			if (hasIntercept) {
				begin = 1;
			}
			for (int j = begin; j < dim; j++) {
				grad.add(j, l2 * weight.get(j));
				hessian.add(j, j, l2);
			}
			if (hasIntercept) {
				grad.add(0, minL2Weight * weight.get(0));
				hessian.add(0, 0, minL2Weight);
			}
			minL2Weight *= 10;

		}
		if (null == dir) {
			throw new RuntimeException("sqp fail to calculate the best dir!");
		}
		// loss, dir, grad, hessian
		return Tuple3.of(dir, grad, hessian);
	}

	public static DenseVector getStartDir(ConstraintObjFunc sqpObjFunc, DenseVector weight,
										  DenseVector icmBias, DenseVector ecmBias) {
		int dim = weight.size();
		DenseMatrix equalityConstraint = sqpObjFunc.equalityConstraint;
		DenseMatrix inequalityConstraint = sqpObjFunc.inequalityConstraint;
		DenseVector equalityItem = sqpObjFunc.equalityItem;
		DenseVector inequalityItem = sqpObjFunc.inequalityItem;

		for (int i = 0; i < inequalityItem.size(); i++) {
			double sum = 0;
			double[] inequalRow = inequalityConstraint.getRow(i);
			for (int j = 0; j < dim; j++) {
				sum += weight.get(j) * inequalRow[j];
			}
			inequalityItem.set(i, icmBias.get(i) - sum);
		}

		for (int i = 0; i < equalityItem.size(); i++) {
			double sum = 0;
			double[] equalRow = equalityConstraint.getRow(i);
			for (int j = 0; j < dim; j++) {
				sum += weight.get(j) * equalRow[j];
			}
			equalityItem.set(i, ecmBias.get(i) - sum);
		}

		return new DenseVector(dim);
	}
}
