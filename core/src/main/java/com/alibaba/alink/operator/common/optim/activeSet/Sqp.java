package com.alibaba.alink.operator.common.optim.activeSet;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.CompareCriterionFunction;
import com.alibaba.alink.common.comqueue.CompleteResultFunction;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.model.ModelParamName;
import com.alibaba.alink.operator.common.optim.Optimizer;
import com.alibaba.alink.operator.common.optim.local.LocalSqp;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateMatrix;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 约束拟牛顿法通过使用拟牛顿更新过程累积关于 KKT 方程的二阶信息来保证超线性收敛。
 * 这些方法通常称为序列二次规划 (SQP) 方法，因为每个主迭代都求解一个 QP 子问题（也称为迭代二次规划、递归二次规划或约束变量度量法）。
 */
public class Sqp extends Optimizer {
	private static final int MAX_FEATURE_NUM = 3000;

	/**
	 * construct function.
	 *
	 * @param objFunc   object function, calc loss and grad.
	 * @param trainData data for training.
	 * @param coefDim   the dimension of features.
	 * @param params    some parameters of optimization method.
	 */
	public Sqp(DataSet <OptimObjFunc> objFunc, DataSet <Tuple3 <Double, Double, Vector>> trainData,
			   DataSet <Integer> coefDim, Params params) {
		super(objFunc, trainData, coefDim, params);
	}

	/**
	 * Solve the following quadratic programing problem:
	 * 每一步都是解决二次规划问题
	 * 参考https://www.mathworks.com/help/optim/ug/constrained-nonlinear-optimization-algorithms_zh_CN.html
	 * <p>
	 * min 0.5 * p^THp + g_k^Tp 泰勒展开二次逼近，g是f在当前点的梯度 H是hession矩阵
	 * s.t. A_i \dot p = b - A_i \dot x_k, where i belongs to equality constraints
	 * A_i \dot p >= b - A_i \dot x_k, where i belongs to inequality constraints
	 */
	@Override
	public DataSet <Tuple2 <DenseVector, double[]>> optimize() {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
		//init model weights.
		this.coefficientVec = this.coefDim.map(new InitialCoef()).withBroadcastSet(this.objFuncSet, "objFunc");
		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.trainData, trainData)
			.initWithBroadcastData(OptimVariable.model, coefficientVec)
			.initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
			.initWithBroadcastData(ConstraintVariable.weightDim, coefDim)
			.add(new InitializeParams())
			.add(new PreallocateVector(OptimVariable.grad, new double[2]))//initial grad
			.add(new PreallocateMatrix(OptimVariable.hessian, MAX_FEATURE_NUM))//initial hessian
			.add(new CalcGradAndHessian())//update grad and hessian with up-to-date coef.
			.add(new AllReduce(OptimVariable.gradHessAllReduce))
			.add(new GetGradientAndHessian())
			.add(new CalcDir(params))//sqp calculate dir
			.add(new LineSearch(params.get(HasL2.L_2)))//line search the best weight, and the best grad and hessian.
			.add(new AllReduce(ConstraintVariable.lossAllReduce))
			.add(new GetMinCoef())
			.add(new CalcConvergence())//consider to put the following three step at the head.
			.setCompareCriterionOfNode0(new IterTermination(params.get(HasEpsilonDefaultAs0000001.EPSILON)))
			.setMaxIter(maxIter)
			.closeWith(new BuildModel())
			.exec();

		return model.mapPartition(new ParseRowModel());
	}

	private static class InitializeParams extends ComputeFunction {

		private static final long serialVersionUID = 3775769468090056172L;

		@Override
		public void calc(ComContext context) {
			if (context.getStepNo() == 1) {
				DenseVector weight = ((List <DenseVector>) context.getObj(OptimVariable.model)).get(0);
				context.putObj(ConstraintVariable.weight, weight);
				context.putObj(ConstraintVariable.minL2Weight, 1e-8);
				context.putObj(ConstraintVariable.linearSearchTimes, 40);
				context.putObj(ConstraintVariable.newtonRetryTime, 12);
				context.putObj(ConstraintVariable.loss, 0.);
				ConstraintObjFunc objFunc =
					(ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
				context.putObj(SqpVariable.icmBias, objFunc.inequalityItem);
				context.putObj(SqpVariable.ecmBias, objFunc.equalityItem);

			}
		}
	}

	//initialize the coef.
	public static double[] phaseOne(double[][] equalMatrix, double[] equalItem,
									double[][] inequalMatrix, double[] inequalItem, int dim) {
		int equalNum = 0;
		if (equalItem != null) {
			equalNum = equalItem.length;
		}

		int inequalNum = 0;
		if (inequalItem != null) {
			inequalNum = inequalItem.length;
		}

		int constraintLength = equalNum + inequalNum;
		//if no constraint, return zeros.
		if (constraintLength == 0) {
			double[] res = new double[dim];
			Arrays.fill(res, 1e-4);
			return res;
		}
		//optimize the phase function, which is 0*x1+0*x2+...z1+z2+...,first place equal, then place inequal.
		double[] objData = new double[dim + constraintLength];
		Arrays.fill(objData, dim, dim + constraintLength, 1);
		// objData is linear function coefficients. eg. objData = [c1, c2, c3] constantTerm = b,
		// then optimize function is c1 * x1 + c2 * x2 + c3 * x3 + b.
		// so, here optimize function is z1 + z2 + ...， where z_i is ith constraint.
		LinearObjectiveFunction objFunc = new LinearObjectiveFunction(objData, 0);
		List <LinearConstraint> cons = new ArrayList <>();
		for (int i = 0; i < equalNum; i++) {
			double[] constraint = new double[dim + constraintLength];
			System.arraycopy(equalMatrix[i], 0, constraint, 0, dim);
			constraint[i + dim] = 1;
			double item = equalItem[i];
			cons.add(new LinearConstraint(constraint, Relationship.EQ, item));
		}
		for (int i = 0; i < inequalNum; i++) {
			double[] constraint = new double[dim + constraintLength];
			System.arraycopy(inequalMatrix[i], 0, constraint, 0, dim);
			constraint[i + dim + equalNum] = 1;
			double item = inequalItem[i];
			cons.add(new LinearConstraint(constraint, Relationship.GEQ, item));
		}
		for (int i = dim; i < dim + constraintLength; i++) {
			double[] constraint = new double[dim + constraintLength];
			constraint[i] = 1;
			cons.add(new LinearConstraint(constraint, Relationship.GEQ, 0));
		}

		LinearConstraintSet conSet = new LinearConstraintSet(cons);
		PointValuePair pair = new SimplexSolver().optimize(objFunc, conSet, GoalType.MINIMIZE);
		double[] res = new double[dim];
		System.arraycopy(pair.getPoint(), 0, res, 0, dim);
		return res;
	}

	public static class InitialCoef extends RichMapFunction <Integer, DenseVector> {
		private static final long serialVersionUID = -1725328800337420019L;
		private double[][] equalMatrix;
		private double[] equalItem;
		private double[][] inequalMatrix;
		private double[] inequalItem;

		@Override
		public void open(Configuration parameters) throws Exception {
			ConstraintObjFunc objFunc =
				(ConstraintObjFunc) getRuntimeContext()
					.getBroadcastVariable("objFunc").get(0);
			this.inequalMatrix = objFunc.inequalityConstraint.getArrayCopy2D();
			this.inequalItem = objFunc.inequalityItem.getData();
			this.equalMatrix = objFunc.equalityConstraint.getArrayCopy2D();
			this.equalItem = objFunc.equalityItem.getData();
		}

		@Override
		public DenseVector map(Integer n) throws Exception {
			return new DenseVector(phaseOne(equalMatrix, equalItem, inequalMatrix, inequalItem, n));
		}
	}

	// solve qp problem use active-set method.
	public static class CalcDir extends ComputeFunction {

		private static final long serialVersionUID = 7694040433763461801L;
		private boolean hasIntercept;
		private double l2;

		public CalcDir(Params params) {
			hasIntercept = params.get(HasWithIntercept.WITH_INTERCEPT);
			l2 = params.get(HasL2.L_2);
		}

		@Override
		public void calc(ComContext context) {
			ConstraintObjFunc objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(
				OptimVariable.objFunc)).get(0);
			Double loss = context.getObj(ConstraintVariable.loss);
			//<grad, <weightSum, loss>
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			DenseVector gradient = grad.f0;
			DenseMatrix hessian = context.getObj(OptimVariable.hessian);
			int dim = ((List <Integer>) context.getObj(ConstraintVariable.weightDim)).get(0);
			final int retryTime = context.getObj(ConstraintVariable.newtonRetryTime);
			final double minL2Weight = context.getObj(ConstraintVariable.minL2Weight);
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			// dir init value [0,0, 0]
			// update in equalityItem and inequalityItem by weight
			DenseVector dir = SqpPai.getStartDir(objFunc, weight,
				context.getObj(SqpVariable.icmBias),
				context.getObj(SqpVariable.ecmBias));
			boolean[] activeSet = SqpPai.getActiveSet(objFunc.inequalityConstraint, objFunc.inequalityItem, dir, dim);

			//<new dir, grad, hession>: activeSet may fail, so little modify grad and hession.
			Tuple3 <DenseVector, DenseVector, DenseMatrix> dirItems =
				SqpPai.calcDir(retryTime, dim, objFunc, dir, weight, hessian, gradient, l2, minL2Weight, hasIntercept,
					activeSet);

			dir = dirItems.f0;
			grad.f0 = dirItems.f1;
			hessian = dirItems.f2;
			context.putObj(OptimVariable.grad, grad);
			context.putObj(OptimVariable.hessian, hessian);
			context.putObj(ConstraintVariable.loss, loss);//grad and hessian has been put in.
			context.putObj(OptimVariable.dir, dir);
		}
	}

	public static class LineSearch extends ComputeFunction {

		private static final long serialVersionUID = 2611682666208211053L;
		private ConstraintObjFunc objFunc;
		private double l2Weight;

		public LineSearch(double l2Weight) {
			this.l2Weight = l2Weight;
		}

		@Override
		public void calc(ComContext context) {
			objFunc = (ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
			DenseVector dir = context.getObj(OptimVariable.dir);
			final int linearSearchTimes = context.getObj(ConstraintVariable.linearSearchTimes);
			final double minL2Weight = context.getObj(ConstraintVariable.minL2Weight);
			if (l2Weight == 0) {
				l2Weight += minL2Weight;
			}
			Iterable <Tuple3 <Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.trainData);
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			double[] losses = objFunc.calcLineSearch(labledVectors, weight, dir, linearSearchTimes, l2Weight);
			context.putObj(ConstraintVariable.lossAllReduce, losses);
		}
	}

	public static class GetMinCoef extends ComputeFunction {

		private static final long serialVersionUID = 239058213400494835L;

		@Override
		public void calc(ComContext context) {
			double[] losses = context.getObj(ConstraintVariable.lossAllReduce);
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			DenseVector dir = context.getObj(OptimVariable.dir);
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			double loss = LocalSqp.lineSearch(losses, weight, grad.f0, dir);
			context.putObj(ConstraintVariable.weight, weight);
			int stepNum = context.getStepNo();
			if (stepNum != 1) {
				context.putObj(ConstraintVariable.lastLoss, context.getObj(ConstraintVariable.loss));
			}
			context.putObj(ConstraintVariable.loss, loss);
		}
	}

	public static class CalcConvergence extends ComputeFunction {

		private static final long serialVersionUID = 1936163292416518616L;

		@Override
		public void calc(ComContext context) {
			//restore items
			ConstraintObjFunc objFunc =
				(ConstraintObjFunc) ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
			objFunc.equalityItem = context.getObj(SqpVariable.ecmBias);
			objFunc.inequalityItem = context.getObj(SqpVariable.icmBias);

			if (context.getStepNo() != 1) {
				double loss = context.getObj(ConstraintVariable.loss);
				double lastLoss = context.getObj(ConstraintVariable.lastLoss);
				int iter = context.getStepNo();
				int lossStep = 5;
				double convergence;
				if (iter <= lossStep) {
					convergence = (lastLoss - loss) / (Math.abs(loss) * iter);
				} else {
					convergence = (lastLoss - loss) / (Math.abs(loss) * lossStep);
				}
				//                if (context.getTaskId() == 0) {
				//                    System.out.println("iter: " + iter + ", convergence: " + convergence);
				//                }
				context.putObj(ConstraintVariable.convergence, convergence);
			}
		}
	}

	public static class CalcGradAndHessian extends ComputeFunction {
		private static final long serialVersionUID = 4760392853024920737L;
		private OptimObjFunc objFunc;

		@Override
		public void calc(ComContext context) {
			Iterable <Tuple3 <Double, Double, Vector>> labledVectors = context.getObj(OptimVariable.trainData);
			//<grad, <weightSum, loss>>
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			DenseMatrix hessian = context.getObj(OptimVariable.hessian);

			int size = grad.f0.size();

			if (objFunc == null) {
				objFunc = ((List <OptimObjFunc>) context.getObj(OptimVariable.objFunc)).get(0);
			}
			//here does not add loss calculation。 return <weightSum, loss>
			Tuple2 <Double, Double> loss = objFunc.calcHessianGradientLoss(labledVectors, weight, hessian, grad.f0);

			/**
			 * prepare buffer vec for allReduce. the last two elements of vec are weight Sum and current loss.
			 */
			double[] buffer = context.getObj(OptimVariable.gradHessAllReduce);
			if (buffer == null) {
				buffer = new double[size + size * size + 2];
				context.putObj(OptimVariable.gradHessAllReduce, buffer);
			}
			for (int i = 0; i < size; ++i) {
				buffer[i] = grad.f0.get(i);
				for (int j = 0; j < size; ++j) {
					buffer[(i + 1) * size + j] = hessian.get(i, j);
				}
			}
			buffer[size + size * size] = loss.f0;
			buffer[size + size * size + 1] = loss.f1;
		}
	}

	public static class GetGradientAndHessian extends ComputeFunction {

		private static final long serialVersionUID = 2724626183370161805L;

		@Override
		public void calc(ComContext context) {
			//<grad, <weightSum, loss>
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			DenseMatrix hessian = context.getObj(OptimVariable.hessian);
			int size = grad.f0.size();
			double[] gradarr = context.getObj(OptimVariable.gradHessAllReduce);
			grad.f1[0] = gradarr[size + size * size];
			for (int i = 0; i < size; ++i) {
				grad.f0.set(i, gradarr[i] / grad.f1[0]);
				for (int j = 0; j < size; ++j) {
					hessian.set(i, j, gradarr[(i + 1) * size + j] / grad.f1[0]);
				}
			}
			grad.f1[0] = gradarr[size + size * size];
			grad.f1[1] = gradarr[size + size * size + 1] / grad.f1[0];
		}
	}

	public static class IterTermination extends CompareCriterionFunction {
		private static final long serialVersionUID = -9142037869276356229L;
		private double epsilon;

		IterTermination(double epsilon) {
			this.epsilon = epsilon;
		}

		@Override
		public boolean calc(ComContext context) {
			if (context.getStepNo() == 1) {
				return false;
			}
			double convergence = context.getObj(ConstraintVariable.convergence);
			;
			return Math.abs(convergence) <= epsilon;
		}
	}

	public static class BuildModel extends CompleteResultFunction {

		private static final long serialVersionUID = -7967444945852772659L;

		@Override
		public List <Row> calc(ComContext context) {
			if (context.getTaskId() != 0) {
				return null;
			}
			DenseVector weight = context.getObj(ConstraintVariable.weight);
			double[] losses = new double[2];
			if (context.containsObj(ConstraintVariable.lastLoss)) {
				losses[0] = context.getObj(ConstraintVariable.lastLoss);
			}
			if (context.containsObj(ConstraintVariable.loss)) {
				losses[1] = context.getObj(ConstraintVariable.loss);
			}
			Params params = new Params();
			params.set(ModelParamName.COEF, weight);
			params.set(ModelParamName.LOSS_CURVE, losses);
			List <Row> model = new ArrayList <>(1);
			model.add(Row.of(params.toJson()));
			return model;
		}
	}

	public static class ParseRowModel extends RichMapPartitionFunction <Row, Tuple2 <DenseVector, double[]>> {

		private static final long serialVersionUID = 7590757264182157832L;

		@Override
		public void mapPartition(Iterable <Row> iterable,
								 Collector <Tuple2 <DenseVector, double[]>> collector) throws Exception {
			DenseVector coefVector = null;
			double[] lossCurve = null;
			int taskId = getRuntimeContext().getIndexOfThisSubtask();
			if (taskId == 0) {
				for (Row row : iterable) {
					Params params = Params.fromJson((String) row.getField(0));
					coefVector = params.get(ModelParamName.COEF);
					lossCurve = params.get(ModelParamName.LOSS_CURVE);
				}

				if (coefVector != null) {
					collector.collect(Tuple2.of(coefVector, lossCurve));
				}
			}
		}
	}
}
