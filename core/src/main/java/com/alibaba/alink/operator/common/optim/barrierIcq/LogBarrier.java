package com.alibaba.alink.operator.common.optim.barrierIcq;

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
import com.alibaba.alink.operator.common.optim.activeSet.SqpPai;
import com.alibaba.alink.operator.common.optim.activeSet.SqpUtil;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateMatrix;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

import java.util.List;

//https://www.stat.cmu.edu/~ryantibs/convexopt-S15/scribes/15-barr-method-scribed.pdf
public class LogBarrier extends Optimizer {
	private static final int MAX_FEATURE_NUM = 3000;

	/**
	 * construct function.
	 *
	 * @param objFunc   object function, calc loss and grad.
	 * @param trainData data for training.
	 * @param coefDim   the dimension of features.
	 * @param params    some parameters of optimization method.
	 */
	public LogBarrier(DataSet <OptimObjFunc> objFunc, DataSet <Tuple3 <Double, Double, Vector>> trainData,
					  DataSet <Integer> coefDim, Params params) {
		super(objFunc, trainData, coefDim, params);
	}

	@Override
	public DataSet <Tuple2 <DenseVector, double[]>> optimize() {
		//use glpk
		//in barrier, maxIter is the local parameter in one loop.
		this.coefficientVec = this.coefDim.map(new Sqp.InitialCoef()).withBroadcastSet(this.objFuncSet, "objFunc");
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.trainData, trainData)
			.initWithBroadcastData(OptimVariable.model, coefficientVec)
			.initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
			.initWithBroadcastData(ConstraintVariable.weightDim, coefDim)
			.add(new InitializeParams())
			.add(new PreallocateVector(OptimVariable.grad, new double[2]))//initial grad
			.add(new PreallocateMatrix(OptimVariable.hessian, MAX_FEATURE_NUM))//initial hessian
			.add(new Sqp.CalcGradAndHessian())//update grad and hessian with up-to-date coef.
			.add(new AllReduce(OptimVariable.gradHessAllReduce))
			.add(new Sqp.GetGradientAndHessian())
			.add(new RunNewtonStep(params))//add constraint and solve the dir
			.add(new Sqp.LineSearch(params.get(HasL2.L_2)))
			.add(new AllReduce(ConstraintVariable.lossAllReduce))
			.add(new Sqp.GetMinCoef())
			.add(new CalcConvergence())
			.setCompareCriterionOfNode0(new IterTermination(maxIter, params.get(HasEpsilonDefaultAs0000001.EPSILON)))
			.closeWith(new Sqp.BuildModel())
			.exec();
		return model.mapPartition(new Sqp.ParseRowModel());
	}

	public static class InitializeParams extends ComputeFunction {

		private static final long serialVersionUID = 1857803292287152190L;

		@Override
		public void calc(ComContext context) {
			if (context.getStepNo() == 1) {
				ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
					OptimVariable.objFunc)).get(0);
				double t = objFunc.inequalityConstraint.numRows();
				double divideT;
				if (t == 0) {
					divideT = 0;
				} else {
					divideT = 1 / t;
				}
				context.putObj(BarrierVariable.t, t);
				context.putObj(BarrierVariable.divideT, divideT);
				context.putObj(BarrierVariable.localIterTime, 0);
				context.putObj(BarrierVariable.hessianNotConvergence, false);
				context.putObj(ConstraintVariable.newtonRetryTime, 12);
				context.putObj(ConstraintVariable.minL2Weight, 1e-8);
				context.putObj(ConstraintVariable.linearSearchTimes, 40);
				DenseVector weight = ((List <DenseVector>) context.getObj(OptimVariable.model)).get(0);
				context.putObj(ConstraintVariable.weight, weight);
				context.putObj(ConstraintVariable.loss, 0.0);
				context.putObj(ConstraintVariable.lastLoss, Double.MAX_VALUE);
			}
		}
	}

	public static class RunNewtonStep extends ComputeFunction {
		private static final long serialVersionUID = 4802057437164571355L;
		private boolean hasIntercept;
		private double l2;

		public RunNewtonStep(Params params) {
			hasIntercept = params.get(HasWithIntercept.WITH_INTERCEPT);
			this.l2 = params.get(HasL2.L_2);
		}

		@Override
		public void calc(ComContext context) {
			ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
				OptimVariable.objFunc)).get(0);
			int hessianDim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
			int begin = 0;
			if (hasIntercept) {
				begin = 1;
			}
			double minL2Weight = context.getObj(ConstraintVariable.minL2Weight);
			Double loss = context.getObj(ConstraintVariable.loss);
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			DenseVector gradient = grad.f0;
			DenseMatrix hessian = context.getObj(ConstraintVariable.hessian);
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			final int retryTime = context.getObj(ConstraintVariable.newtonRetryTime);
			double t = context.getObj(BarrierVariable.t);
			int constraintNum = objFunc.equalityItem.size() + objFunc.inequalityItem.size();
			addInequalityConstraint(objFunc, constraintNum, gradient, weight, hessian, t);
			for (int j = 0; j < retryTime; j++) {
				try {
					int hSize = objFunc.equalityItem.size() + hessianDim;
					DenseVector g = new DenseVector(hSize);
					SqpPai.vecAddVec(gradient, g, hessianDim);
					DenseMatrix h = new DenseMatrix(
						buildH(hessian, weight, g, objFunc.equalityConstraint, objFunc.equalityItem));
					double norm = 1 / g.normL1();
					h.scaleEqual(norm);
					g.scaleEqual(norm);
					DenseVector dir = new DenseVector(hessianDim);
					SqpPai.vecAddVec(h.inverse().multiplies(g), dir, hessianDim);
					context.putObj(ConstraintVariable.dir, dir);
					break;
				} catch (Exception e) {
					double l2Weight = l2 + 2;
					for (int i = begin; i < hessianDim; i++) {
						loss += 0.5 * l2Weight * Math.pow(weight.get(i), 2);
						gradient.add(i, l2Weight * weight.get(i));
						hessian.add(i, i, l2Weight);
					}
					if (hasIntercept) {
						loss += 0.5 * minL2Weight * Math.pow(weight.get(0), 2);
						gradient.add(0, minL2Weight * weight.get(0));
						hessian.add(0, 0, minL2Weight);
					}
					minL2Weight *= 10;
				}
			}
			grad.f0 = gradient;
			context.putObj(ConstraintVariable.grad, grad);
			context.putObj(ConstraintVariable.hessian, hessian);
			loss = constrainedLoss(loss, weight, objFunc, t, constraintNum);
			context.putObj(ConstraintVariable.loss, loss);
		}

		private static double[][] buildH(DenseMatrix hessian, DenseVector weight, DenseVector g,
										 DenseMatrix equalityConstraint, DenseVector equalityItem) {
			int equalNum = equalityItem.size();
			int dim = weight.size();
			int hSize = equalNum + dim;
			double[][] h = new double[hSize][hSize];
			double[][] hessianArray = hessian.getArrayCopy2D();
			SqpUtil.fillMatrix(h, 0, 0, hessianArray);
			for (int i = 0; i < equalNum; i++) {
				int row = i + dim;
				double sum = 0;
				for (int j = 0; j < dim; j++) {
					h[row][j] = equalityConstraint.get(i, j);
					h[j][row] = equalityConstraint.get(i, j);
					sum += equalityConstraint.get(i, j) * weight.get(j);
				}
				g.set(row, sum - equalityItem.get(i));
			}

			return h;
		}

		private static double constrainedLoss(double loss, DenseVector weight, ConstraintObjFunc objFunc,
											  double t, double constraintNum) {
			if (constraintNum == 0) {
				return loss;
			}
			int numRow = objFunc.inequalityConstraint.numRows();
			for (int i = 0; i < numRow; i++) {
				loss -= t * Math.log(sumInequality(objFunc.inequalityConstraint, objFunc.inequalityItem, weight, i));
			}
			return loss;
		}

		private static double sumInequality(DenseMatrix icm, DenseVector icb, DenseVector w, int row) {
			double[] wData = w.getData();
			double s = icb.get(row);
			int colNum = icm.numCols();
			for (int i = 0; i < colNum; i++) {
				s -= wData[i] * icm.get(row, i);
			}
			if (s == 0) {
				s = 1e-6;
			}
			return s;
		}

		private static void addInequalityConstraint(ConstraintObjFunc objFunc, int constraintNum, DenseVector gradient,
													DenseVector weight, DenseMatrix hessian, double t) {
			if (constraintNum == 0) {
				return;
			}
			updateGradForInequalityConstraint(objFunc, gradient, weight, hessian, 1 / t);
		}

		private static void updateGradForInequalityConstraint(ConstraintObjFunc objFunc, DenseVector gradient,
															  DenseVector weight, DenseMatrix hessian, double t) {
			DenseMatrix icm = objFunc.inequalityConstraint;
			int rowNum = icm.numRows();
			int colNum = icm.numCols();
			for (int k = 0; k < rowNum; k++) {
				double sum = sumInequality(icm, objFunc.inequalityItem, weight, k);
				for (int i = 0; i < colNum; i++) {
					double val = icm.get(k, i);
					gradient.add(i, t * val / sum);
				}
			}
			for (int k = 0; k < rowNum; k++) {
				double sum = Math.pow(sumInequality(icm, objFunc.inequalityItem, weight, k), 2);
				for (int i = 0; i < colNum; i++) {
					double val1 = icm.get(k, i);
					for (int j = 0; j < colNum; j++) {
						double val2 = icm.get(k, j);
						hessian.add(i, j, t * val1 * val2 / sum);
					}
				}
			}
		}

	}

	public static class CalcConvergence extends ComputeFunction {

		private static final long serialVersionUID = 4453719204627742833L;

		@Override
		public void calc(ComContext context) {
			int iter = context.getObj(BarrierVariable.localIterTime);
			double convergence;
			if (iter == 0) {
				convergence = 100;
			} else {
				double lastLoss = context.getObj(ConstraintVariable.lastLoss);
				double loss = context.getObj(ConstraintVariable.loss);
				int lossStep = 5;
				if (iter <= lossStep) {
					convergence = (lastLoss - loss) / (Math.abs(loss) * iter);
				} else {
					convergence = (lastLoss - loss) / (Math.abs(loss) * lossStep);
				}
			}
			context.putObj(BarrierVariable.localIterTime, iter + 1);
			//            if (context.getTaskId() == 0) {
			//                System.out.println("iter: " + iter + ", convergence: " + convergence);
			//            }
			context.putObj(ConstraintVariable.convergence, convergence);
		}
	}

	public static class IterTermination extends CompareCriterionFunction {
		private static final long serialVersionUID = -3313706116254321792L;
		private int maxIter;
		private double epsilon;

		IterTermination(int maxIter, double epsilon) {
			this.maxIter = maxIter;
			this.epsilon = epsilon;
		}

		@Override
		public boolean calc(ComContext context) {
			double convergence = context.getObj(ConstraintVariable.convergence);
			if (convergence < this.epsilon ||
				(int) context.getObj(BarrierVariable.localIterTime) >= maxIter) {
				context.putObj(BarrierVariable.localIterTime, 0);
				double t = context.getObj(BarrierVariable.t);
				double divideT = context.getObj(BarrierVariable.divideT);
				t *= 50;
				divideT /= 50;
				context.putObj(BarrierVariable.t, t);
				context.putObj(BarrierVariable.divideT, divideT);
				return divideT < this.epsilon;
			}
			return false;
		}
	}
}
