package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;

public class AutoCrossObjFunc extends OptimObjFunc {
	private static final long serialVersionUID = 4901712689789156832L;
	private LogLossFunc lossFunc;

	public AutoCrossObjFunc(Params params) {
		super(params);
		lossFunc = new LogLossFunc();
	}

	//index, label, vectorData. index indices the start of calculating coef. allCoefVector saves all the coefs.
	@Override
	public double calcLoss(Tuple3 <Double, Double, Vector> labelVector, DenseVector allCoefVector) {
		double eta = getEta(labelVector, allCoefVector);
		return this.lossFunc.loss(eta, labelVector.f1);
	}

	//需要将fixed的param的数目记录下来，从而可以稀疏地更新grad。
	@Override
	public void updateGradient(Tuple3 <Double, Double, Vector> labelVector,
								  DenseVector allCoefVector,
								  DenseVector updateGrad) {
		double eta = getEta(labelVector, allCoefVector);
		double div = lossFunc.derivative(eta, labelVector.f1);
		int fixedCoefSize = allCoefVector.size() - updateGrad.size();
		double[] grad = updateGrad.getData();
		SparseVector sv = (SparseVector) labelVector.f2;
		//grad只是需要更新的梯度部分。
		for (int i = 0; i < sv.getIndices().length; i++) {
			int index = sv.getIndices()[i];
			if (index >= fixedCoefSize) {
				grad[index - fixedCoefSize] += div * sv.getValues()[i];
			}
		}
	}

	@Override
	public void updateHessian(Tuple3 <Double, Double, Vector> labelVector,
								 DenseVector coefVector,
								 DenseMatrix updateHessian) {
		throw new RuntimeException("do not support hessian.");
	}

	@Override
	public boolean hasSecondDerivative() {
		return false;
	}

	@Override
	public double[] calcSearchValues(Iterable <Tuple3 <Double, Double, Vector>> labelVectors,
									 DenseVector allCoefVector,//fixed+candidate
									 DenseVector dirVec,//candidate
									 double beta,
									 int numStep) {
		double[] vec = new double[numStep + 1];

		int fixedSize = allCoefVector.size() - dirVec.size();
		double[] realDir = new double[allCoefVector.size()];
		System.arraycopy(dirVec.getData(), 0, realDir, fixedSize, dirVec.size());
		DenseVector realDirVec = new DenseVector(realDir);
		for (Tuple3 <Double, Double, Vector> labelVector : labelVectors) {
			//            //cancat
			//            if (index == -1) {
			//                index = (int) Math.round(labelVector.f0);
			//                double[] realDir = allCoefVector.getData();
			//                System.arraycopy(dirVec.getData(), 0, realDir, index, dirVec.size());
			//                realDirVec = new DenseVector(realDir);
			//            }

			double weight = labelVector.f0;
			double etaCoef = getEta(labelVector, allCoefVector);
			double etaDelta = getEta(labelVector, realDirVec) * beta;
			for (int i = 0; i < numStep + 1; ++i) {
				vec[i] += weight * lossFunc.loss(etaCoef - i * etaDelta, labelVector.f1);
			}
		}
		return vec;
	}

	private double getEta(Tuple3 <Double, Double, Vector> labelVector, DenseVector coefVector) {
		return MatVecOp.dot(labelVector.f2, coefVector);
	}
}
