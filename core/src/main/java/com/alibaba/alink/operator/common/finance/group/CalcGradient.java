package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;

import java.util.Arrays;
import java.util.List;

/**
 * Calculate gradient from labelVectors. labelVectors are stored in static memory. we use allReduce communication
 * pattern instead of map and broadcast. this class will be used by Lbfgs, Gd, Owlqn.
 */
public class CalcGradient extends ComputeFunction {
	/**
	 * object function class, it supply the functions to calc local gradient (or loss).
	 */
	private OptimObjFunc objFunc;

	@Override
	public void calc(ComContext context) {
		//System.out.println("CalcGradient: " + context.getStepNo());
		if (CalcStep.LR_TRAIN_LEFT != context.getObj(GroupScoreCardVariable.NEXT_STEP) &&
			CalcStep.LR_TRAIN_RIGHT != context.getObj(GroupScoreCardVariable.NEXT_STEP)) {
			return;
		}

		Iterable <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> labledVectors =
			context.getObj(OptimVariable.trainData);
		List <Integer> selectedDataIndices = context.getObj(GroupScoreCardVariable.SELECTED_DATA_INDICES);

		// get iterative coefficient from static memory.
		Tuple2 <DenseVector, Double> state = context.getObj(OptimVariable.currentCoef);
		int size = state.f0.size();
		DenseVector coef = state.f0;
		if (objFunc == null) {
			objFunc = ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
		}
		Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.dir);
		// calculate local gradient
		double weightSum = calcGradient(labledVectors, selectedDataIndices, coef, grad.f0);

		// prepare buffer vec for allReduce. the last element of vec is the weight Sum.
		double[] buffer = context.getObj(OptimVariable.gradAllReduce);
		if (buffer == null) {
			buffer = new double[size + 1];
			context.putObj(OptimVariable.gradAllReduce, buffer);
		}

		for (int i = 0; i < size; ++i) {
			buffer[i] = grad.f0.get(i) * weightSum;
		}

		/* the last element is the weight value */
		buffer[size] = weightSum;
	}

	public double calcGradient(Iterable <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> labelVectors,
							   List <Integer> selectedDataIndices,
							   DenseVector coefVector,
							   DenseVector grad) {
		double weightSum = 0.0;
		Arrays.fill(grad.getData(), 0.0);
		int idx = 0;
		int selectedIdx = 0;
		for (Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double> labelVector : labelVectors) {
			if (selectedIdx >= selectedDataIndices.size()) {
				break;
			}
			if (idx == selectedDataIndices.get(selectedIdx)) {
				if (labelVector.f1.f2 instanceof SparseVector) {
					((SparseVector) (labelVector.f1.f2)).setSize(coefVector.size());
				}
				weightSum += labelVector.f1.f0;
				objFunc.updateGradient(labelVector.f1, coefVector, grad);
				selectedIdx++;
			}
			idx++;
		}

		finalizeGradient(coefVector, grad, weightSum);

		return weightSum;
	}

	public void finalizeGradient(DenseVector coefVector, DenseVector grad, double weightSum) {
		if (weightSum > 0.0) {
			grad.scaleEqual(1.0 / weightSum);
		}
		//if (0.0 != this.l2) {
		//	grad.plusScaleEqual(coefVector, this.l2 * 2);
		//}
		//if (0.0 != this.l1) {
		//	double[] coefArray = coefVector.getData();
		//	for (int i = 0; i < coefVector.size(); i++) {
		//		grad.add(i, Math.signum(coefArray[i]) * this.l1);
		//	}
		//}

	}
}
