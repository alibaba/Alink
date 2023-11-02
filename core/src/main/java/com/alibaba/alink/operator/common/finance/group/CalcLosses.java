package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.params.shared.linear.LinearTrainParams.OptimMethod;

import java.util.List;

/**
 * Calculate losses from labelVectors. labelVectors are stored in static memory. we use allReduce communication
 * pattern instead of map and broadcast. this class will be used by Lbfgs, Gd, Owlqn.
 */
public class CalcLosses extends ComputeFunction {
	/**
	 * object function class, it supply the functions to calc gradient (or loss) locality.
	 */
	private OptimObjFunc objFunc;
	private final OptimMethod method;
	private final int numSearchStep;

	/**
	 * calc losses.
	 *
	 * @param method        optimization method.
	 * @param numSearchStep num search step in line search.
	 */
	public CalcLosses(OptimMethod method, int numSearchStep) {
		this.method = method;
		this.numSearchStep = numSearchStep;
	}

	@Override
	public void calc(ComContext context) {
		//System.out.println("CalcLosses: " + context.getStepNo());
		if (CalcStep.LR_TRAIN_LEFT != context.getObj(GroupScoreCardVariable.NEXT_STEP) &&
			CalcStep.LR_TRAIN_RIGHT != context.getObj(GroupScoreCardVariable.NEXT_STEP)) {
			return;
		}

		List <Integer> selectedDataIndices = context.getObj(GroupScoreCardVariable.SELECTED_DATA_INDICES);
		List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> labledVectors
			= context.getObj(OptimVariable.trainData);
		Tuple2 <DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
		Tuple2 <DenseVector, Double> coef = context.getObj(OptimVariable.currentCoef);
		if (objFunc == null) {
			objFunc = ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
		}

		Double beta = dir.f1[1] / numSearchStep;
		double[] vec = method.equals(OptimMethod.OWLQN) ?
			constraintCalcSearchValues(labledVectors, selectedDataIndices, coef.f0, dir.f0, beta, numSearchStep)
			: calcSearchValues(labledVectors, selectedDataIndices, coef.f0, dir.f0, beta, numSearchStep);

		// prepare buffer vec for allReduce.
		double[] buffer = context.getObj(OptimVariable.lossAllReduce);
		if (buffer == null) {
			buffer = vec.clone();
			context.putObj(OptimVariable.lossAllReduce, buffer);
		} else {
			System.arraycopy(vec, 0, buffer, 0, vec.length);
		}
	}

	public double[] constraintCalcSearchValues(
		List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> labelVectors,
		List <Integer> selectedDataIndices,
		DenseVector coefVector,
		DenseVector dirVec,
		double beta,
		int numStep) {
		double[] losses = new double[numStep + 1];
		double[] coefArray = coefVector.getData();
		double[] dirArray = dirVec.getData();
		int size = coefArray.length;
		DenseVector newCoef = new DenseVector(size);
		double[] newCoefArray = newCoef.getData();
		for (int i = 0; i < numStep + 1; ++i) {
			double tmpVal = beta * i;
			for (int s = 0; s < size; ++s) {
				double val = coefArray[s] - tmpVal * dirArray[s];
				if (val * coefArray[s] < 0) {
					val = 0.0;
				}
				newCoefArray[s] = val;
			}
			int idx = 0;
			int selectedIdx = 0;
			for (Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double> labelVector : labelVectors) {
				if (idx == selectedDataIndices.get(selectedIdx)) {
					losses[i] += objFunc.calcLoss(labelVector.f1, newCoef) * labelVector.f1.f0;
					selectedIdx++;
				}
				idx++;
			}
		}
		return losses;
	}

	public double[] calcSearchValues(List <Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double>> labelVectors,
									 List <Integer> selectedDataIndices,
									 DenseVector coefVector,
									 DenseVector dirVec,
									 double beta,
									 int numStep) {
		double[] losses = new double[numStep + 1];
		DenseVector[] stepVec = new DenseVector[numStep + 1];
		stepVec[0] = coefVector.clone();
		DenseVector vecDelta = dirVec.scale(beta);
		for (int i = 1; i < numStep + 1; i++) {
			stepVec[i] = stepVec[i - 1].minus(vecDelta);
		}
		for (int i = 0; i < numStep + 1; ++i) {
			int idx = 0;
			int selectedIdx = 0;
			for (Tuple3 <Row, Tuple3 <Double, Double, Vector>, Double> labelVector : labelVectors) {
				if (selectedIdx >= selectedDataIndices.size()) {
					break;
				}
				if (idx == selectedDataIndices.get(selectedIdx)) {
					losses[i] += objFunc.calcLoss(labelVector.f1, stepVec[i]) * labelVector.f1.f0;
					selectedIdx++;
				}
				idx++;
			}
		}
		return losses;
	}
}
