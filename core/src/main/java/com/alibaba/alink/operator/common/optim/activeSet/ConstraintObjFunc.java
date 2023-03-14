package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.UnaryLossObjFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;

public class ConstraintObjFunc extends UnaryLossObjFunc {

	private static final long serialVersionUID = -8984917852066425449L;
	public DenseMatrix equalityConstraint;
	public DenseMatrix inequalityConstraint;
	public DenseVector equalityItem;
	public DenseVector inequalityItem;

	/**
	 * Constructor.
	 *
	 * @param unaryLossFunc loss function.
	 * @param params        input parameters.
	 */
	public ConstraintObjFunc(UnaryLossFunc unaryLossFunc, Params params) {
		super(unaryLossFunc, params);
	}

	/**
	 * Calculate loss values for line search in optimization.
	 *
	 * @param labelVectors train data.
	 * @param coefVector   coefficient of current time.
	 * @param dirVecOrigin descend direction of optimization problem.
	 * @param numStep      num of line search step.
	 * @return double[] losses.
	 */
	public double[] calcLineSearch(Iterable <Tuple3 <Double, Double, Vector>> labelVectors, DenseVector coefVector,
								   DenseVector dirVecOrigin, int numStep, double l2Weight) {
		double[] losses = new double[2 * numStep + 1];
		DenseVector[] stepVec = new DenseVector[2 * numStep + 1];
		stepVec[numStep] = coefVector.clone();
		DenseVector dirVec = dirVecOrigin.clone();
		stepVec[numStep + 1] = coefVector.plus(dirVec);
		stepVec[numStep - 1] = coefVector.minus(dirVec);
		for (int i = 2; i < numStep + 1; i++) {
			DenseVector temp = dirVec.scale(9 * Math.pow(10, 1 - i));
			stepVec[numStep + i] = stepVec[numStep + i - 1].minus(temp);
			stepVec[numStep - i] = stepVec[numStep - i + 1].plus(temp);
		}

		double l2Item = coefVector.normL2() * l2Weight;
		for (Tuple3 <Double, Double, Vector> labelVector : labelVectors) {
			for (int i = 0; i < numStep * 2 + 1; ++i) {
				losses[i] += calcLoss(labelVector, stepVec[i]) * labelVector.f0 + l2Item;
			}
		}
		return losses;
	}

}
