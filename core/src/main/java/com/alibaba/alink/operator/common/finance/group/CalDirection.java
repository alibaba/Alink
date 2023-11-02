package com.alibaba.alink.operator.common.finance.group;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.comqueue.ComContext;
import com.alibaba.alink.common.comqueue.ComputeFunction;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.optim.subfunc.OptimVariable;

public class CalDirection
	extends ComputeFunction {
	private transient DenseVector oldGradient;
	private transient double[] alpha;
	private final int m;

	public CalDirection(int numCorrections) {
		m = numCorrections;
	}

	@Override
	public void calc(ComContext context) {
		//System.out.println("CalDirection: " + context.getStepNo());
		if (CalcStep.LR_TRAIN_LEFT != context.getObj(GroupScoreCardVariable.NEXT_STEP) &&
			CalcStep.LR_TRAIN_RIGHT != context.getObj(GroupScoreCardVariable.NEXT_STEP)) {
			return;
		}

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
		int l = Math.min(k, m);
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
