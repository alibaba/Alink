package com.alibaba.alink.operator.common.optim.local;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintObjFunc;
import com.alibaba.alink.operator.common.optim.activeSet.SqpPai;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import java.util.List;

public class LocalSqp {
	public static Tuple2 <DenseVector, Double> sqp(List <Tuple3 <Double, Double, Vector>> labledVectors,
												   DenseVector initCoefs,
												   Params params, OptimObjFunc objFunc) {
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> hessianItem = sqpWithHessian(labledVectors, initCoefs,
			objFunc, params);
		return Tuple2.of(hessianItem.f0, hessianItem.f3);
	}

	static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double>
	sqpWithHessian(List <Tuple3 <Double, Double, Vector>> labledVectors,
				   DenseVector initCoefs, OptimObjFunc objFunc,
				   Params params) {
		double l2 = params.get(HasL2.L_2);
		double l1 = params.get(HasL1.L_1);
		boolean hasIntercept = params.get(LinearTrainParams.WITH_INTERCEPT);
		int dim = initCoefs.size();
		DenseVector weight = initCoefs;
		DenseMatrix hessian = new DenseMatrix(dim, dim);
		DenseVector grad = new DenseVector(dim);
		double loss = 0;
		double lastLoss = -1;
		//update grad and hessian
		ConstraintObjFunc sqpObjFunc = (ConstraintObjFunc) objFunc;
		final int retryTime = 12;
		final double minL2Weight = 1e-8;
		final int linearSearchTimes = 40;
		//store constraint bias
		if (sqpObjFunc.inequalityConstraint == null) {
			sqpObjFunc.inequalityConstraint = new DenseMatrix(0, dim);
			sqpObjFunc.inequalityItem = new DenseVector(0);
		}
		if (sqpObjFunc.equalityConstraint == null) {
			sqpObjFunc.equalityConstraint = new DenseMatrix(0, dim);
			sqpObjFunc.equalityItem = new DenseVector(0);
		}
		DenseVector icmBias = sqpObjFunc.inequalityItem.clone();
		DenseVector ecmBias = sqpObjFunc.equalityItem.clone();

		//sqp iteration
		for (int sqpIter = 0; sqpIter < 100; sqpIter++) {
			double weightSumCoef = 1 / sqpObjFunc.calcHessianGradientLoss(labledVectors, weight, hessian, grad).f0;
			grad.scaleEqual(weightSumCoef);
			hessian.scaleEqual(weightSumCoef);
			//initial for each iteration
			DenseVector dir = SqpPai.getStartDir(sqpObjFunc, weight, icmBias, ecmBias);
			boolean[] activeSet = SqpPai.getActiveSet(sqpObjFunc.inequalityConstraint, sqpObjFunc.inequalityItem, dir,
				dim);
			//get the current best direction
			Tuple3 <DenseVector, DenseVector, DenseMatrix> dirItems =
				SqpPai.calcDir(retryTime, dim, sqpObjFunc, dir, weight,
					hessian, grad, l2, minL2Weight, hasIntercept, activeSet);
			dir = dirItems.f0;
			grad = dirItems.f1;
			hessian = dirItems.f2;
			//linear search
			double[] losses = sqpObjFunc.calcLineSearch(labledVectors, weight, dir, linearSearchTimes,
				l2 + minL2Weight);
			loss = lineSearch(losses, weight, grad, dir);
			//check convergence
			if (sqpIter != 0) {
				lastLoss = loss;
			}
			//restore item
			sqpObjFunc.inequalityItem = icmBias;
			sqpObjFunc.equalityItem = ecmBias;
			double convergence = 100;
			if (sqpIter != 0) {
				int lossStep = 5;
				if (sqpIter <= lossStep) {
					convergence = (lastLoss - loss) / (Math.abs(loss) * sqpIter);
				} else {
					convergence = (lastLoss - loss) / (Math.abs(loss) * lossStep);
				}
			}
			if (convergence <= 1e-6) {
				break;
			}
		}
		return Tuple4.of(weight, grad, hessian, loss);
	}

	public static Double lineSearch(double[] losses, DenseVector weight, DenseVector grad, DenseVector dir) {
		double beta = 1e-4;
		double alpha = 1;
		double backOff = 0.1;
		int i = 1;
		int index = 0;
		int size = losses.length;
		int origin = size / 2;
		int retryTime = origin - 1;
		double gd = -BLAS.dot(grad, dir);
		double betaTemp = beta * gd * alpha;
		boolean brea = false;
		for (i = 1; i < retryTime; i++) {
			index = origin - i;
			if (losses[index] <= losses[origin] + betaTemp) {
				brea = true;
				break;
			}
			//            index = origin + i;
			//            if (losses[index] <= losses[origin] + betaTemp) {
			//                brea = true;
			//                break;
			//            }
			betaTemp *= backOff;
		}
		if (!brea) {
			betaTemp = beta * gd * alpha;
			for (i = 1; i < retryTime; i++) {
				index = origin + i;
				if (losses[index] <= losses[origin] + betaTemp) {
					break;
				}
				betaTemp *= backOff;
			}
		}
		if (index < origin) {
			weight.minusEqual(dir.scale(Math.pow(10, 1 - i)));
		} else {
			weight.plusEqual(dir.scale(Math.pow(10, 1 - i)));
		}
		return losses[index];
	}

	//    public static Tuple3<DenseVector, DenseVector, DenseMatrix> calcDir(
	//            double retryTime, int dim, ConstraintObjFunc sqpObjFunc, DenseVector weight,
	//            DenseMatrix hessian, DenseVector grad, double loss,
	//            double l2Weight, double minL2Weight, boolean hasIntercept) {
	//        DenseVector dir = null;
	//        DenseMatrix equalityConstraint = sqpObjFunc.equalityConstraint;
	//        DenseMatrix inequalityConstraint = sqpObjFunc.inequalityConstraint;
	//        DenseVector equalityItem = sqpObjFunc.equalityItem;
	//        DenseVector inequalityItem = sqpObjFunc.inequalityItem;
	//        for (int i = 0; i < retryTime; i++) {
	//
	//            DenseVector x0 = new DenseVector(dim);
	//            try {
	//                dir = QpProblem.qpact(hessian, grad,
	//                        equalityConstraint, equalityItem,
	//                        inequalityConstraint, inequalityItem,
	//                        x0).f0;
	//                break;
	//            } catch (Exception e) {
	//                //update gradient and hessian
	//                int begin = 0;
	//                double l2 = l2Weight + minL2Weight;
	//                if (hasIntercept) {
	//                    begin = 1;
	//                }
	//                for (int j = begin; j < dim; j++) {
	////                    loss += 0.5 * l2 * Math.pow(weight.get(j), 2);
	//                    grad.add(j, l2 * weight.get(j));
	//                    hessian.add(j, j, l2);
	//                }
	//                if (hasIntercept) {
	////                    loss += 0.5 * minL2Weight * Math.pow(weight.get(0), 2);
	//                    grad.add(0, minL2Weight * weight.get(0));
	//                    hessian.add(0, 0, minL2Weight);
	//                }
	//                minL2Weight *= 10;
	//            }
	//        }
	//        if (null == dir) {
	//            throw new RuntimeException("sqp fail to calculate the best dir!");
	//        }
	//        // loss, dir, grad, hessian
	//        return Tuple3.of(dir, grad, hessian);
	//    }
}