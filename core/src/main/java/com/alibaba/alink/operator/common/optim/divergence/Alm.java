package com.alibaba.alink.operator.common.optim.divergence;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.Optimizer;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintObjFunc;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintVariable;
import com.alibaba.alink.operator.common.optim.activeSet.Sqp;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Alm extends Optimizer {
	/**
	 * Construct function.
	 *
	 * @param objFunc        The object function, calc loss and grad.
	 * @param trainData      THe data for training.
	 * @param coefficientDim The dimension of features.
	 * @param params         Some parameters of optimization method.
	 */
	public Alm(DataSet <OptimObjFunc> objFunc, DataSet <Tuple3 <Double, Double, Vector>> trainData,
			   DataSet <Integer> coefficientDim, Params params) {
		super(objFunc, trainData, coefficientDim, params);
	}

	@Override
	public DataSet <Tuple2 <DenseVector, double[]>> optimize() {
		// Initializes the weight.
		this.coefficientVec = this.coefDim.map(new InitialWeight());

		//
		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.trainData, trainData)
			.initWithBroadcastData(OptimVariable.model, coefficientVec)
			.initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
			.initWithBroadcastData(ConstraintVariable.weightDim, coefDim)
			.add(new InitialAlm())
			.add(new CalcLossAndGrad())
			.add(new AllReduce(AlmVariable.gradVariableForReduce))
			.add(new UpdateLossAndGrad())
			.add(new LinearSearch())
			.add(new AllReduce(AlmVariable.lossVariableForReduce))
			.add(new UpdateModel())// calculates the best alpha, dir and weight, just in iter = 0
			.add(new CalcConvergence())
			.add(new UpdateOperationOfK())//if k plus 1, then updates lambda, multiples mu, checks algo convergence.
			.setCompareCriterionOfNode0(new IterTermination())
			.closeWith(new Sqp.BuildModel())
			.exec();

		return model.mapPartition(new Sqp.ParseRowModel());
	}

	private static class InitialWeight implements MapFunction <Integer, DenseVector> {
		private static final long serialVersionUID = 5545720465892186449L;

		@Override
		public DenseVector map(Integer dim) throws Exception {
			DenseVector weight = new DenseVector(dim);
			Random r = new Random(1);
			for (int i = 0; i < dim; i++) {
				weight.set(i, r.nextInt(10) * 0.1);
			}
			return weight;
		}
	}

	private static class InitialAlm extends ComputeFunction {

		private static final long serialVersionUID = -8274197490784405325L;

		@Override
		public void calc(ComContext context) {
			if (context.getStepNo() == 1) {
				context.putObj(AlmVariable.weight, ((List <DenseVector>) context.getObj(OptimVariable.model)).get(0));
				context.putObj(AlmVariable.oldK, -1);
				context.putObj(AlmVariable.k, 0);
				context.putObj(AlmVariable.iter, 0);//in this algo, judge iter with this param.

				context.putObj(AlmVariable.minIterTime, 500);
				context.putObj(AlmVariable.maxKIterStep, 15);
				context.putObj(AlmVariable.retryNum, 40);
				context.putObj(AlmVariable.convergenceTolerance, 1e-6);
				context.putObj(AlmVariable.kConvergence, 15);

				ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
					OptimVariable.objFunc)).get(0);
				DenseMatrix equalMatrix = objFunc.equalityConstraint;
				int emSize = equalMatrix.numRows();
				int numCol = equalMatrix.numCols();

				/* normalize equality constraint */
				for (int i = 0; i < emSize; i++) {
					double maxCo = 0;
					for (int j = 0; j < numCol; j++) {
						maxCo = Math.max(maxCo, Math.abs(equalMatrix.get(i, j)));
					}
					for (int j = 0; j < numCol; j++) {
						equalMatrix.set(i, j, equalMatrix.get(i, j) / maxCo);
					}
				}

				int imSize = objFunc.inequalityConstraint.numRows();
				int lambdaSize = emSize + imSize;
				context.putObj(AlmVariable.lambdaSize, lambdaSize);
				double[] lambda = new double[lambdaSize];
				Arrays.fill(lambda, 1e-6);

				context.putObj(AlmVariable.mu, 1e-6);
				context.putObj(AlmVariable.lambda, lambda);
				context.putObj(AlmVariable.convergence, 100.0);
				context.putObj(AlmVariable.alpha, 0.1);
			}
			if ((int) context.getObj(AlmVariable.iter) == 0) {
				int dim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
				context.putObj(AlmVariable.dir, new DenseVector(dim));
				context.putObj(ConstraintVariable.loss, 0.);
				context.putObj(ConstraintVariable.lastLoss, -1.);
				context.putObj(AlmVariable.c, -1);

				//initial convergence
				context.putObj(AlmVariable.lastValidIter, -1);
				context.putObj(AlmVariable.numInvalid, 0);
			}
		}
	}

	private static class CalcLossAndGrad extends ComputeFunction {

		private static final long serialVersionUID = -3617672711509308525L;

		@Override
		public void calc(ComContext context) {
			Iterable <Tuple3 <Double, Double, Vector>> labeledVectors = context.getObj(OptimVariable.trainData);
			DenseVector weight = context.getObj(AlmVariable.weight);
			double[] paramsForReduce = DivergenceFunction.calcLossDivergenceAndGrad(labeledVectors, weight);
			context.putObj(AlmVariable.gradVariableForReduce, paramsForReduce);
		}
	}

	private static class UpdateLossAndGrad extends ComputeFunction {

		private static final long serialVersionUID = -5565962450487064673L;

		@Override
		public void calc(ComContext context) {
			double[] params = context.getObj(AlmVariable.gradVariableForReduce);
			double scoreLeft = params[0];
			double scoreRight = params[1];
			double scoreSquareLeft = params[2];
			double scoreSquareRight = params[3];
			double nLeft = params[4];
			double nRight = params[5];

			double eLeft;
			double eRight;
			double vLeft;
			double vRight;

			//下面计算的时候，vLeft和vRight互为相反数。variance不应该为负数。
			eLeft = scoreLeft / nLeft;
			vLeft = scoreSquareLeft / nLeft - Math.pow(eLeft, 2);
			eRight = scoreRight / nRight;
			vRight = scoreSquareRight / nRight - Math.pow(eRight, 2);

			context.putObj(ConstraintVariable.lastLoss, context.getObj(ConstraintVariable.loss));
			double loss = -Math.pow(eLeft - eRight, 2) / (vLeft + vRight);
			int dim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
			double[] scoreXLeft = Arrays.copyOfRange(params, 6, 6 + dim);
			double[] scoreXRight = Arrays.copyOfRange(params, 6 + dim, 6 + 2 * dim);
			double[] eDirLeft = Arrays.copyOfRange(params, 6 + 2 * dim, 6 + 3 * dim);
			double[] eDirRight = Arrays.copyOfRange(params, 6 + 3 * dim, 6 + 4 * dim);

			double eDiff = eLeft - eRight;
			double vSum = vLeft + vRight;
			double diffDivSum = eDiff / vSum;
			double diff2DivSum2 = Math.pow(diffDivSum, 2);
			double[] vDirLeft = new double[dim];
			double[] vDirRight = new double[dim];
			DenseVector gradient = new DenseVector(dim);

			for (int i = 0; i < dim; i++) {
				eDirLeft[i] /= nLeft;
				eDirRight[i] /= nRight;
				vDirLeft[i] = (scoreXLeft[i] / nLeft - eLeft * eDirLeft[i]) * 2;
				vDirRight[i] = (scoreXRight[i] / nRight - eRight * eDirRight[i]) * 2;
				double g1 = -2 * diffDivSum * (eDirLeft[i] - eDirRight[i]);
				double g2 = diff2DivSum2 * (vDirLeft[i] + vDirRight[i]);
				gradient.set(i, g1 + g2);
			}

			ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
				OptimVariable.objFunc)).get(0);
			DenseVector weight = context.getObj(AlmVariable.weight);
			double[] optLambda = context.getObj(AlmVariable.lambda);
			double mu = context.getObj(AlmVariable.mu);
			double equal = updateGradForEqualityPenalty(objFunc.equalityConstraint, objFunc.equalityItem,
				weight, gradient, optLambda, mu, true, dim);

			double inEqualLoss = updateGradForInequalityPenalty(objFunc.inequalityConstraint, objFunc.inequalityItem,
				weight, gradient, optLambda, mu, true, dim, objFunc.equalityConstraint.numRows());
			loss += equal;
			loss += inEqualLoss;

			context.putObj(ConstraintVariable.loss, loss);
			if (context.getTaskId() == 0) {
				if (context.getStepNo() % 20 == 0) {
					System.out.println("step: " + context.getStepNo() + " loss " + loss);
				}
			}
			context.putObj(ConstraintVariable.grad, gradient);

			//here is checkout point. put it here to save place.
			int iter = context.getObj(AlmVariable.iter);
			double c = context.getObj(AlmVariable.convergence);
			if (iter == 0 || c > 0) {
				context.putObj(AlmVariable.oldWeight, weight);
				context.putObj(AlmVariable.oldDir, context.getObj(AlmVariable.dir));
				context.putObj(AlmVariable.numInvalid, 0);
			}
		}
	}

	private static class LinearSearch extends ComputeFunction {

		private static final long serialVersionUID = 845765787010061045L;

		@Override
		public void calc(ComContext context) {
			if ((int) context.getObj(AlmVariable.iter) == 0) {
				DenseVector gradient = context.getObj(ConstraintVariable.grad);
				double alpha = context.getObj(AlmVariable.alpha);
				DenseVector weight = context.getObj(AlmVariable.weight);
				Iterable <Tuple3 <Double, Double, Vector>> labeledVectors = context.getObj(OptimVariable.trainData);
				DenseVector dir = gradient.scale(alpha);

				int retryNum = context.getObj(AlmVariable.retryNum);
				DenseVector[] weights = new DenseVector[retryNum];
				weights[0] = weight.minus(dir);
				for (int i = 1; i < retryNum; i++) {
					/* scale backOff */
					dir.scaleEqual(0.1);
					weights[i] = weight.minus(dir);
				}

				double[] lossVariableForReduce = DivergenceFunction.calcLossDivergence(labeledVectors, weights,
					retryNum);
				context.putObj(AlmVariable.lossVariableForReduce, lossVariableForReduce);
			} else {
				context.putObj(AlmVariable.lossVariableForReduce, new double[] {0});
			}
		}
	}

	//here only choose the best dir and weight, do not update loss
	private static class UpdateModel extends ComputeFunction {

		private static final long serialVersionUID = -6147219895343590554L;

		@Override
		public void calc(ComContext context) {
			DenseVector weight = context.getObj(AlmVariable.weight);
			DenseVector gradient = context.getObj(ConstraintVariable.grad);
			double alpha = context.getObj(AlmVariable.alpha);
			if ((int) context.getObj(AlmVariable.iter) == 0) {
				int retryNum = context.getObj(AlmVariable.retryNum);
				double[] params = context.getObj(AlmVariable.lossVariableForReduce);
				double[] scoreLeft = new double[retryNum];
				double[] scoreRight = new double[retryNum];

				double[] scoreSquareLeft = new double[retryNum];
				double[] scoreSquareRight = new double[retryNum];
				double[] nLeft = new double[retryNum];
				double[] nRight = new double[retryNum];

				System.arraycopy(params, 0, scoreLeft, 0, retryNum);
				System.arraycopy(params, retryNum, scoreRight, 0, retryNum);
				System.arraycopy(params, 2 * retryNum, scoreSquareLeft, 0, retryNum);
				System.arraycopy(params, 3 * retryNum, scoreSquareRight, 0, retryNum);
				System.arraycopy(params, 4 * retryNum, nLeft, 0, retryNum);
				System.arraycopy(params, 5 * retryNum, nRight, 0, retryNum);

				double eLeft;
				double eRight;
				double vLeft;
				double vRight;

				//update loss with constraint
				int dim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
				ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
					OptimVariable.objFunc)).get(0);
				double[] optLambda = context.getObj(AlmVariable.lambda);
				double mu = context.getObj(AlmVariable.mu);

				double loss = 0;
				loss += updateGradForEqualityPenalty(objFunc.equalityConstraint, objFunc.equalityItem,
					weight, gradient, optLambda, mu, false, dim);
				loss += updateGradForInequalityPenalty(objFunc.inequalityConstraint, objFunc.inequalityItem,
					weight, gradient, optLambda, mu, false, dim, objFunc.equalityConstraint.numRows());
				double[] losses = new double[retryNum];

				Arrays.fill(losses, loss);
				for (int i = 0; i < retryNum; i++) {
					eLeft = scoreLeft[i] / nLeft[i];
					eRight = scoreRight[i] / nRight[i];
					vLeft = scoreSquareLeft[i] / nLeft[i] - Math.pow(eLeft, 2);
					vRight = scoreSquareRight[i] / nRight[i] - Math.pow(eRight, 2);
					losses[i] = -Math.pow(eLeft - eRight, 2) / (vLeft + vRight);
				}

				// calculate the best alpha, weight and dir.
				double oldLoss = context.getObj(ConstraintVariable.loss);
				double gd = 0;
				gd -= gradient.normL2Square();
				double beta = 1e-4;
				gd *= beta;
				double backOff = 0.1;
				int index = 0;

				for (; index < retryNum; index++) {
					if (losses[index] <= oldLoss + gd * alpha) {
						break;
					}
					alpha *= backOff;
				}

				DenseVector dir = gradient.scale(alpha);
				context.putObj(AlmVariable.dir, dir);

				weight.minusEqual(dir);
				context.putObj(OptimVariable.dir, dir);
			} else {
				DenseVector dir = context.getObj(OptimVariable.dir);
				dir.scaleEqual(0.9);
				dir.minusEqual(gradient.scale(alpha));
				weight.plusEqual(dir);
				context.putObj(OptimVariable.dir, dir);
			}
			context.putObj(AlmVariable.weight, weight);
		}
	}

	private static class CalcConvergence extends ComputeFunction {
		private static final long serialVersionUID = 6500706606298759318L;

		@Override
		public void calc(ComContext context) {
			int iter = context.getObj(AlmVariable.iter);
			if (iter == (int) context.getObj(AlmVariable.minIterTime)) {//return if meet the max iteration time.
				System.out.println("check convergence at step: " + context.getStepNo());
				context.putObj(AlmVariable.iter, 0);
				context.putObj(AlmVariable.k, (int) context.getObj(AlmVariable.k) + 1);
				return;
			}
			if (iter != 0) {
				double loss = context.getObj(ConstraintVariable.loss);
				double lastLoss = context.getObj(ConstraintVariable.lastLoss);
				int lossStep = 5;

				double convergence;
				if (iter <= lossStep) {
					convergence = (lastLoss - loss) / (Math.abs(loss) * iter);
				} else {
					convergence = (lastLoss - loss) / (Math.abs(loss) * lossStep);
				}
				context.putObj(AlmVariable.convergence, convergence);
				if (convergence >= 0 && convergence < (double) context.getObj(AlmVariable.convergenceTolerance)) {
					context.putObj(AlmVariable.iter, 0);
					context.putObj(AlmVariable.k, (int) context.getObj(AlmVariable.k) + 1);
					return;
				}
				int numInvalid = context.getObj(AlmVariable.numInvalid);
				DenseVector dir = context.getObj(AlmVariable.dir);
				if (convergence > 0) {
					//checkout point
					context.putObj(AlmVariable.oldWeight, context.getObj(AlmVariable.weight));
					context.putObj(AlmVariable.oldDir, dir);
					context.putObj(AlmVariable.lastValidIter, context.getObj(AlmVariable.iter));
					context.putObj(AlmVariable.numInvalid, 0);
				}
				if (convergence < -0.1) {
					dir.scaleEqual(0);
					if (numInvalid < 5) {
						numInvalid++;
					} else {
						//recover
						context.putObj(AlmVariable.alpha, 0.1 * (double) context.getObj(AlmVariable.alpha));
						System.out.println("recover alpha : " + context.getObj(AlmVariable.alpha));
						context.putObj(AlmVariable.iter, (int) context.getObj(AlmVariable.lastValidIter) - 1);
						context.putObj(AlmVariable.weight, context.getObj(AlmVariable.oldWeight));
						context.putObj(AlmVariable.dir, context.getObj(AlmVariable.oldDir));
					}
					context.putObj(AlmVariable.numInvalid, numInvalid);
				}
			} else {
				//iter 0 check point
				context.putObj(AlmVariable.oldWeight, context.getObj(AlmVariable.weight));
				context.putObj(AlmVariable.lastValidIter, context.getObj(AlmVariable.iter));
				context.putObj(AlmVariable.numInvalid, 0);
			}
			context.putObj(AlmVariable.iter, 1 + (int) context.getObj(AlmVariable.iter));
		}
	}

	private static class UpdateOperationOfK extends ComputeFunction {

		private static final long serialVersionUID = -6329053429856505L;

		@Override
		public void calc(ComContext context) {
			int k = context.getObj(AlmVariable.k);
			int oldK = context.getObj(AlmVariable.oldK);
			if (k - oldK == 2) {
				//update lambda
				double maxIneqViolation = 0;
				ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
					OptimVariable.objFunc)).get(0);
				DenseMatrix equalMatrix = objFunc.equalityConstraint;
				DenseMatrix inEqualMatrix = objFunc.inequalityConstraint;

				int equalNumRow = equalMatrix.numRows();
				int inEqualNumRow = inEqualMatrix.numRows();
				DenseVector weight = context.getObj(AlmVariable.weight);
				int dim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
				double[] optLambda = context.getObj(AlmVariable.lambda);
				double mu = context.getObj(AlmVariable.mu);
				double equalityResidual = 0;

				if (equalNumRow != 0) {
					for (int i = 0; i < equalNumRow; i++) {
						double cI = constraintFunc(equalMatrix, objFunc.equalityItem, weight, i, dim, true);
						equalityResidual += Math.abs(cI);
						optLambda[i] -= cI * mu;
					}
					equalityResidual /= equalNumRow;
				}

				double inequalityResidual = 0;
				if (inEqualNumRow != 0) {
					for (int i = 0; i < inEqualNumRow; i++) {
						double cI = constraintFunc(inEqualMatrix, objFunc.inequalityItem, weight, i, dim, false);
						optLambda[equalNumRow + i] -= cI * mu;
						optLambda[equalNumRow + i] = Math.max(optLambda[equalNumRow + i], 0);
						if (cI < 0) {
							maxIneqViolation = Math.max(maxIneqViolation, Math.abs(cI));
							inequalityResidual += cI;
						}
					}
					inequalityResidual /= inEqualNumRow;
				}

				//update mu
				mu *= 50;
				context.putObj(AlmVariable.mu, mu);
				context.putObj(AlmVariable.lambda, optLambda);
				context.putObj(AlmVariable.equalityResidual, equalityResidual);
				context.putObj(AlmVariable.inequalityResidual, inequalityResidual);
			}
		}
	}

	private static class IterTermination extends CompareCriterionFunction {

		private static final long serialVersionUID = 956073123419389295L;

		@Override
		public boolean calc(ComContext context) {
			int k = context.getObj(AlmVariable.k);
			if (k == (int) context.getObj(AlmVariable.maxKIterStep)) {
				if (context.getTaskId() == 0) {
					System.out.println("Stop at the maxKIterStep: " + context.getObj(AlmVariable.maxKIterStep));
				}
				return true;
			}
			int oldK = context.getObj(AlmVariable.oldK);

			if (k - oldK == 2) {
				/* that means no constraints. */
				if (oldK == -1 && (int) context.getObj(AlmVariable.lambdaSize) == 0) {
					return true;
				}
				context.putObj(AlmVariable.oldK, oldK + 1);

				double equalityResidual = context.getObj(AlmVariable.equalityResidual);
				double inequalityResidual = context.getObj(AlmVariable.inequalityResidual);

				System.out.println(
					"kIter: " + k + " loss : [" + equalityResidual + ", " + inequalityResidual + "] alpha : "
						+ context.getObj(AlmVariable.alpha));
				return equalityResidual < 1e-3 && Math.abs(inequalityResidual) < 1e-3;
			}
			return false;
		}
	}

	/* the equalityFunc and inequalityFunc are mainly the same, so just add isEqual. */
	private static double constraintFunc(DenseMatrix matrix, DenseVector item, DenseVector weight,
										 int i, int dim, boolean isEqual) {
		double residual = 0;
		double[] rowValue = matrix.getRow(i);
		for (int j = 0; j < dim; j++) {
			residual += weight.get(j) * rowValue[j];
		}
		if (isEqual) {
			residual -= item.get(i);
		} else {
			residual = item.get(i) - residual;
		}
		return residual;
	}

	//update grad if updateGrad, else not update anything.
	private static double updateGradForEqualityPenalty(DenseMatrix equalMatrix, DenseVector equalItem,
													   DenseVector weight, DenseVector gradient,
													   double[] optLambda, double mu, boolean updateGrad, int dim) {
		double lambdaLoss = 0;
		double penaltyLoss = 0;
		int numRow = equalMatrix.numRows();
		for (int i = 0; i < numRow; i++) {
			double lambda = optLambda[i];
			double residual = constraintFunc(equalMatrix, equalItem, weight, i, dim, true);
			if (updateGrad) {
				for (int j = 0; j < dim; j++) {
					gradient.add(j, mu * residual * equalMatrix.get(i, j));
					gradient.add(j, -lambda * equalMatrix.get(i, j));
				}
			}
			lambdaLoss -= lambda * residual;
			penaltyLoss += 0.5 * mu * Math.pow(residual, 2);
		}
		return lambdaLoss + penaltyLoss;
	}

	private static double updateGradForInequalityPenalty(DenseMatrix inEqualMatrix, DenseVector inEqualItem,
														 DenseVector weight, DenseVector gradient,
														 double[] optLambda, double mu, boolean updateGrad, int dim,
														 int emSize) {
		int numRow = inEqualMatrix.numRows();
		double lambdaLoss = 0;
		double penaltyLoss = 0;

		for (int i = 0; i < numRow; i++) {
			double cI = constraintFunc(inEqualMatrix, inEqualItem, weight, i, dim, false);
			double lambdaI = optLambda[emSize + i];
			double cond = cI - lambdaI / mu;
			if (cond > 0) {
				lambdaLoss += 0.5 / mu * Math.pow(lambdaI, 2);
			} else {
				lambdaLoss -= lambdaI * cI;
				penaltyLoss = 0.5 * mu * Math.pow(cI, 2);
				if (updateGrad) {
					for (int j = 0; j < dim; j++) {
						gradient.add(j, lambdaI * inEqualMatrix.get(i, j));
						gradient.add(j, -mu * cI * inEqualMatrix.get(i, j));
					}
				}
			}
		}
		return lambdaLoss + penaltyLoss;
	}
}
