package com.alibaba.alink.operator.common.optim;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.optim.subfunc.CalcGradient;
import com.alibaba.alink.operator.common.optim.subfunc.CalcLosses;
import com.alibaba.alink.operator.common.optim.subfunc.IterTermination;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;
import com.alibaba.alink.operator.common.optim.subfunc.OutputModel;
import com.alibaba.alink.operator.common.optim.subfunc.ParseRowModel;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateCoefficient;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateLossCurve;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateSkyk;
import com.alibaba.alink.operator.common.optim.subfunc.PreallocateVector;
import com.alibaba.alink.operator.common.optim.subfunc.UpdateModel;
import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.comqueue.IterativeComQueue;
import com.alibaba.alink.common.comqueue.communication.AllReduce;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.optim.HasNumSearchStepDv4;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * The optimizer of lbfgs.
 */
public class Lbfgs extends Optimizer {

	/**
	 * construct function.
	 *
	 * @param objFunc   object function, calc loss and grad.
	 * @param trainData data for training.
	 * @param coefDim   the dimension of features.
	 * @param params    some parameters of optimization method.
	 */
	public Lbfgs(DataSet<OptimObjFunc> objFunc, DataSet <Tuple3 <Double, Double, Vector>> trainData,
				 DataSet <Integer> coefDim, Params params) {
		super(objFunc, trainData, coefDim, params);
	}

	/**
	 * optimizer api.
	 *
	 * @return the coefficient of linear problem.
	 */
	@Override
	public DataSet <Tuple2 <DenseVector, double[]>> optimize() {
		//get parameters.
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
		int numSearchStep = params.get(HasNumSearchStepDv4.NUM_SEARCH_STEP);
		if (null == this.coefVec) {
			initCoefZeros();
		}

		/**
		 * solving problem using iteration.
		 * trainData is the distributed samples.
		 * initCoef is the initial model coefficient, which will be broadcast to every worker.
		 * objFuncSet is the object function in dataSet format
		 * .add(new PreallocateCoefficient(OptimName.currentCoef)) allocate memory for current coefficient
		 * .add(new PreallocateCoefficient(OptimName.minCoef))     allocate memory for min loss coefficient
		 * .add(new PreallocateLossCurve(OptimVariable.lossCurve)) allocate memory for loss values
		 * .add(new PreallocateVector(OptimName.dir ...))          allocate memory for dir
		 * .add(new PreallocateVector(OptimName.grad))             allocate memory for grad
		 * .add(new PreallocateSkyk())                             allocate memory for sK yK
		 * .add(new CalcGradient(objFunc))                         calculate local sub gradient
		 * .add(new AllReduce(OptimName.gradAllReduce))            sum all sub gradient with allReduce
		 * .add(new CalDirection())                                get summed gradient and use it to calc descend dir
		 * .add(new CalcLosses(objFunc, OptimMethod.GD))           calculate local losses for line search
		 * .add(new AllReduce(OptimName.lossAllReduce))            sum all losses with allReduce
		 * .add(new UpdateModel(maxIter, epsilon ...))             update coefficient
		 * .setCompareCriterionOfNode0(new IterTermination())             judge stop of iteration
		 */
		DataSet <Row> model = new IterativeComQueue()
			.initWithPartitionedData(OptimVariable.trainData, trainData)
			.initWithBroadcastData(OptimVariable.model, coefVec)
			.initWithBroadcastData(OptimVariable.objFunc, objFuncSet)
			.add(new PreallocateCoefficient(OptimVariable.currentCoef))
			.add(new PreallocateCoefficient(OptimVariable.minCoef))
			.add(new PreallocateLossCurve(OptimVariable.lossCurve, maxIter))
			.add(new PreallocateVector(OptimVariable.dir, new double[] {0.0, OptimVariable.learningRate}))
			.add(new PreallocateVector(OptimVariable.grad))
			.add(new PreallocateSkyk(OptimVariable.numCorrections))
			.add(new CalcGradient())
			.add(new AllReduce(OptimVariable.gradAllReduce))
			.add(new CalDirection(OptimVariable.numCorrections))
			.add(new CalcLosses(OptimMethod.LBFGS, numSearchStep))
			.add(new AllReduce(OptimVariable.lossAllReduce))
			.add(new UpdateModel(params, OptimVariable.grad, OptimMethod.LBFGS, numSearchStep))
			.setCompareCriterionOfNode0(new IterTermination())
			.closeWith(new OutputModel())
			.setMaxIter(maxIter)
			.exec();

		return model.mapPartition(new ParseRowModel());
	}

	/**
	 * calculate the descend direction of lbfgs.
	 */
	public static class CalDirection
		extends ComputeFunction {
		private transient DenseVector oldGradient;
		private transient double[] alpha;
		private int m;

		private CalDirection(int numCorrections) {
			m = numCorrections;
		}

		@Override
		public void calc(ComContext context) {
			Tuple2 <DenseVector, double[]> grad = context.getObj(OptimVariable.grad);
			Tuple2 <DenseVector, double[]> dir = context.getObj(OptimVariable.dir);
			Tuple2 <DenseVector[], DenseVector[]> hessian = context.getObj(OptimVariable.sKyK);
			int size = grad.f0.size();
			double[] gradarr = context.getObj(OptimVariable.gradAllReduce);

			if (this.oldGradient == null) {
				oldGradient = new DenseVector(size);
			}
			DenseVector[] sK = hessian.f0;
			DenseVector[] yK = hessian.f1;
			for (int i = 0; i < size; ++i) {
				grad.f0.set(i, gradarr[i] / gradarr[size]);
			}

			dir.f1[0] = gradarr[size];
			int k = context.getStepNo() - 1;

			if (k == 0) {
				dir.f0.setEqual(grad.f0);
				oldGradient.setEqual(grad.f0);
			} else {
				yK[(k - 1) % m].setEqual(grad.f0);
				yK[(k - 1) % m].minusEqual(oldGradient);
				oldGradient.setEqual(grad.f0);
			}
			// copy g_k and store in qL

			dir.f0.setEqual(grad.f0);

			// compute H^-1 * g_k
			int delta = k > m ? k - m : 0;
			int l = k <= m ? k : m;
			if (alpha == null) {
				alpha = new double[m];
			}
			for (int i = l - 1; i >= 0; i--) {
				int j = (i + delta) % m;
				double dot = sK[j].dot(yK[j]);
				if (Math.abs(dot) > 0.0) {
					double rhoJ = 1.0 / dot;
					alpha[i] = rhoJ * (sK[j].dot(dir.f0));
					dir.f0.plusScaleEqual(yK[j], -alpha[i]);
				}
			}
			for (int i = 0; i < l; i++) {
				int j = (i + delta) % m;
				double dot = sK[j].dot(yK[j]);
				if (Math.abs(dot) > 0.0) {
					double rhoJ = 1.0 / dot;
					double betaI = rhoJ * (yK[j].dot(dir.f0));
					dir.f0.plusScaleEqual(sK[j], (alpha[i] - betaI));
				}
			}
		}
	}
}