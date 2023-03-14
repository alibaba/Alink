package com.alibaba.alink.operator.common.linear;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.jama.JMatrixFunc;
import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.common.probabilistic.PDF;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.optim.local.ConstrainedLocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.common.regression.LinearRegressionModel;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.feature.HasConstraint;
import com.alibaba.alink.params.finance.HasConstrainedOptimizationMethod;
import com.alibaba.alink.params.regression.LinearRegPredictParams;
import com.alibaba.alink.params.regression.LinearRegTrainParams;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams.OptimMethod;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LocalLinearModel {

	public static ModelSummary trainWithSummary(List <Tuple3 <Double, Double, Vector>> trainData,
												int[] indices,
												LinearModelType modelType,
												String optimMethod,
												boolean hasIntercept,
												boolean standardization,
												String constraint,
												double l1,
												double l2,
												BaseVectorSummarizer srt) {
		if (indices == null) {
			int size = trainData.get(0).f2.size();
			indices = new int[size];
			for (int i = 0; i < indices.length; i++) {
				indices[i] = i;
			}
		}

		//        if (modelType == LinearModelType.LinearReg && l1 <= 0 && l2 <= 0) {
		//            //train with srt.
		//            return null;
		//        } else {

		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model = train(trainData, indices, modelType,
			optimMethod, hasIntercept, standardization, constraint, l1, l2);
		return calcModelSummary(model, srt, modelType, indices);
		//        }
	}

	public static ModelSummary calcModelSummary(Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model,
												BaseVectorSummarizer srt,
												LinearModelType modelType,
												int[] indices) {

		if (modelType == LinearModelType.LR) {
			return calcLrSummary(model, srt);
		} else {
			return calcLinearRegressionSummary(model, srt, 0, indicesAddOne(indices));
		}
	}

	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> train(
		List <Tuple3 <Double, Double, Vector>> trainData,
		int[] indices,
		LinearModelType modelType,
		OptimMethod optimMethod,
		boolean hasIntercept,
		boolean standardization,
		double l1,
		double l2) {
		int featureSize = indices.length;
		List <Tuple3 <Double, Double, Vector>> selectedData;
		if (hasIntercept) {
			selectedData = new ArrayList <>();
			for (Tuple3 <Double, Double, Vector> data : trainData) {
				selectedData.add(Tuple3.of(data.f0, data.f1, data.f2.slice(indices).prefix(1.0)));
			}
		} else {
			selectedData = trainData;
		}
		final OptimObjFunc objFunc = OptimObjFunc.getObjFunction(modelType, new Params());

		Params optParams = new Params()
			.set(LinearTrainParams.OPTIM_METHOD,
				ParamUtil.searchEnum(LinearTrainParams.OPTIM_METHOD, optimMethod.name()))
			.set(LinearTrainParams.WITH_INTERCEPT, hasIntercept)
			.set(LinearTrainParams.STANDARDIZATION, standardization)
			.set(HasL1.L_1, l1)
			.set(HasL2.L_2, l2);

		DenseVector initialWeights = DenseVector.zeros(featureSize + (hasIntercept ? 1 : 0));

		Tuple4 <DenseVector, DenseVector, DenseMatrix, double[]> temp;
		if (optimMethod == OptimMethod.Newton) {
			try {
				temp = LocalOptimizer.newtonWithHessian(selectedData, initialWeights, optParams, objFunc);
			} catch (Exception e) {
				throw new RuntimeException("Local trainLinear failed.", e);
			}
		} else {
			try {
				Tuple2 <DenseVector, double[]> tuple2 = LocalOptimizer.optimize(objFunc, selectedData, initialWeights,
					optParams);

				Params newtonParams = new Params()
					.set(LinearTrainParams.OPTIM_METHOD, ParamUtil.searchEnum(LinearTrainParams.OPTIM_METHOD,
						"newton"))
					.set(LinearTrainParams.WITH_INTERCEPT, hasIntercept)
					.set(LinearTrainParams.STANDARDIZATION, standardization)
					.set(HasL1.L_1, l1)
					.set(HasL2.L_2, l2)
					.set(LinearRegTrainParams.MAX_ITER, 1);
				temp = LocalOptimizer.newtonWithHessian(selectedData, tuple2.f0, newtonParams, objFunc);
			} catch (Exception e) {
				throw new RuntimeException("Local trainLinear failed.", e);
			}
		}

		return Tuple4.of(temp.f0, temp.f1, temp.f2, temp.f3[temp.f3.length - 3]);
	}

	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> constrainedTrain(
		List <Tuple3 <Double, Double, Vector>> trainData,
		int[] indices,
		LinearModelType modelType,
		HasConstrainedOptimizationMethod.ConstOptimMethod optimMethod,
		boolean hasIntercept,
		boolean standardization,
		String constraint,
		double l1,
		double l2) {
		List <Tuple3 <Double, Double, Vector>> selectedData;
		if (hasIntercept) {
			selectedData = new ArrayList <>();
			for (Tuple3 <Double, Double, Vector> data : trainData) {
				selectedData.add(Tuple3.of(data.f0, data.f1, data.f2.slice(indices).prefix(1.0)));
			}
		} else {
			selectedData = trainData;
		}
		Params optParams = new Params()
			.set(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD,
				ParamUtil.searchEnum(HasConstrainedOptimizationMethod.CONST_OPTIM_METHOD, optimMethod.name()))
			.set(LinearTrainParams.WITH_INTERCEPT, hasIntercept)
			.set(LinearTrainParams.STANDARDIZATION, standardization)
			.set(HasL1.L_1, l1)
			.set(HasL2.L_2, l2)
			.set(HasConstraint.CONSTRAINT, constraint);

		if (optimMethod == HasConstrainedOptimizationMethod.ConstOptimMethod.SQP ||
			optimMethod == HasConstrainedOptimizationMethod.ConstOptimMethod.Barrier) {
			return ConstrainedLocalOptimizer.optimizeWithHessian(selectedData, modelType, optParams);
		} else {
			throw new RuntimeException("It is not support for constrainedTrain");
		}
	}

	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> train(
		List <Tuple3 <Double, Double, Vector>> trainData,
		int[] indices,
		LinearModelType modelType,
		String optimMethod,
		boolean hasIntercept,
		boolean standardization,
		String constraint,
		double l1,
		double l2) {
		String optimMethodUper = optimMethod.toUpperCase().trim();
		if ("SQP".equals(optimMethodUper) || "BARRIER".equals(optimMethodUper)) {
			return constrainedTrain(trainData,
				indices,
				modelType,
				HasConstrainedOptimizationMethod.ConstOptimMethod.valueOf(optimMethodUper),
				hasIntercept,
				standardization,
				constraint,
				l1,
				l2);
		} else {
			return train(trainData,
				indices,
				modelType,
				OptimMethod.valueOf(optimMethodUper),
				hasIntercept,
				standardization,
				l1,
				l2);
		}

	}

	private static LinearRegressionSummary calcLinearRegressionSummary(
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> model,
		BaseVectorSummarizer srt,
		int indexY,
		int[] indexX) {
		LinearRegressionSummary summary = calcLinearRegressionSummary(model.f0, srt, indexY, indexX);
		summary.gradient = model.f1;
		summary.hessian = model.f2;
		summary.loss = model.f3;
		return summary;

	}

	static LinearRegressionSummary calcLinearRegressionSummary(DenseVector beta,
															   BaseVectorSummarizer srt,
															   int indexY,
															   int[] indexX) {
		BaseVectorSummary summary = srt.toSummary();

		if (summary.count() == 0) {
			throw new RuntimeException("table is empty!");
		}

		if (summary.vectorSize() < indexX.length) {
			throw new RuntimeException("record size Less than features size!");
		}

		int nx = indexX.length;
		long N = summary.count();
		if (N == 0) {
			throw new RuntimeException("Y valid value num is zero!");
		}

		String nameY = "label";
		String[] nameX = new String[indexX.length];
		Arrays.fill(nameX, "col");

		LinearRegressionModel lrr = new LinearRegressionModel(N, nameY, nameX);

		double[] XBar = new double[nx];
		for (int i = 0; i < nx; i++) {
			XBar[i] = summary.mean(indexX[i]);
		}
		double yBar = summary.mean(indexY);

		double[][] cov = srt.covariance().getArrayCopy2D();
		DenseMatrix dot = srt.getOuterProduct();

		DenseMatrix A = new DenseMatrix(nx, nx);
		for (int i = 0; i < nx; i++) {
			for (int j = 0; j < nx; j++) {
				A.set(i, j, cov[indexX[i]][indexX[j]]);
			}
		}
		DenseMatrix C = new DenseMatrix(nx, 1);
		for (int i = 0; i < nx; i++) {
			C.set(i, 0, cov[indexX[i]][indexY]);
		}

		lrr.beta = beta.getData();

		double S = summary.variance(indexY) * (summary.count() - 1);
		double alpha = lrr.beta[0] - yBar;
		double U = 0.0;
		U += alpha * alpha * N;
		for (int i = 0; i < nx; i++) {
			U += 2 * alpha * summary.sum(indexX[i]) * lrr.beta[i + 1];
		}
		for (int i = 0; i < nx; i++) {
			for (int j = 0; j < nx; j++) {
				U += lrr.beta[i + 1] * lrr.beta[j + 1] * (cov[indexX[i]][indexX[j]] * (N - 1) + summary.mean(indexX[i])
					* summary.mean(indexX[j]) * N);
			}
		}

		double ms = summary.normL2(indexY);
		for (int i = 0; i < nx && indexX[i] < dot.numCols(); i++) {
			ms -= 2 * lrr.beta[i + 1] * dot.get(indexY, indexX[i]);
		}
		ms -= 2 * lrr.beta[0] * summary.sum(indexY);

		for (int i = 0; i < nx; i++) {
			for (int j = 0; j < nx; j++) {
				if (indexX[i] < dot.numCols() && indexX[j] < dot.numCols()) {
					ms += lrr.beta[i + 1] * lrr.beta[j + 1] * dot.get(indexX[i], indexX[j]);
				}
			}
			ms += 2 * lrr.beta[i + 1] * lrr.beta[0] * summary.sum(indexX[i]);
		}

		ms += summary.count() * lrr.beta[0] * lrr.beta[0];

		lrr.SST = S;
		lrr.SSR = U;
		lrr.SSE = S - U;
		if (lrr.SSE < 0) {
			lrr.SSE = ms;
		}
		lrr.dfSST = N - 1;
		lrr.dfSSR = nx;
		lrr.dfSSE = N - nx - 1 - 1; //1 is intercept
		lrr.R2 = Math.max(0.0, Math.min(1.0, lrr.SSR / lrr.SST));
		lrr.R = Math.sqrt(lrr.R2);
		lrr.MST = lrr.SST / lrr.dfSST;
		lrr.MSR = lrr.SSR / lrr.dfSSR;
		lrr.MSE = lrr.SSE / lrr.dfSSE;
		lrr.Ra2 = 1 - lrr.MSE / lrr.MST;
		lrr.s = Math.sqrt(lrr.MSE);
		lrr.F = lrr.MSR / lrr.MSE;
		if (lrr.F < 0) {
			lrr.F = 0;
		}
		lrr.AIC = N * Math.log(lrr.SSE) + 2 * nx;

		A.scaleEqual(N - 1);

		DenseMatrix invA = A.solveLS(JMatrixFunc.identity(A.numRows(), A.numRows()));

		for (int i = 0; i < nx; i++) {
			lrr.FX[i] = lrr.beta[i + 1] * lrr.beta[i + 1] / (lrr.MSE * invA.get(i, i));
			lrr.TX[i] = lrr.beta[i + 1] / (lrr.s * Math.sqrt(invA.get(i, i)));
		}

		int p = nameX.length;
		double df2 = N - p - 1;
		if (df2 <= 0) {
			df2 = N - 2;
		}

		lrr.pEquation = 1 - CDF.F(lrr.F, p, df2);
		lrr.pX = new double[nx];
		for (int i = 0; i < nx; i++) {
			lrr.pX[i] = (1 - CDF.studentT(Math.abs(lrr.TX[i]), df2)) * 2;
		}

		LinearRegressionSummary lrSummary = new LinearRegressionSummary();

		lrSummary.count = summary.count();
		lrSummary.beta = beta;
		lrSummary.fValue = lrr.F;
		lrSummary.mallowCp = lrr.getCp(indexX.length, lrr.SSE);
		lrSummary.r2 = lrr.R2;
		lrSummary.ra2 = lrr.Ra2;
		lrSummary.pValue = lrr.pEquation;
		lrSummary.tValues = lrr.TX;
		lrSummary.tPVaues = lrr.pX;
		lrSummary.sse = lrr.SSE;
		lrSummary.stdEsts = new double[indexX.length];
		lrSummary.stdErrs = new double[indexX.length];
		lrSummary.lowerConfidence = new double[indexX.length];
		lrSummary.uperConfidence = new double[indexX.length];
		for (int i = 0; i < indexX.length; i++) {
			double estimate = lrSummary.beta.get(i + 1);
			lrSummary.stdEsts[i] = estimate * summary.standardDeviation(indexX[i]);
			lrSummary.stdErrs[i] = lrr.s * Math.sqrt(invA.get(i, i));
			lrSummary.lowerConfidence[i] = estimate - 1.96 * lrSummary.stdErrs[i];
			lrSummary.uperConfidence[i] = estimate + 1.96 * lrSummary.stdErrs[i];
		}

		return lrSummary;
	}

	public static LogistRegressionSummary calcLrSummary(
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> weightsAndHessian,
		BaseVectorSummarizer srt) {
		DenseVector weights = weightsAndHessian.f0;
		DenseVector gradient = weightsAndHessian.f1;
		DenseMatrix hessian = weightsAndHessian.f2;
		double loss = weightsAndHessian.f3;

		int featureNum = gradient.size() - 1;

		LogistRegressionSummary summary = new LogistRegressionSummary();

		summary.loss = loss;
		summary.gradient = weightsAndHessian.f1;
		summary.hessian = weightsAndHessian.f2;
		summary.beta = weightsAndHessian.f0;

		summary.scoreChiSquareValue = hessian.solveLS(gradient).dot(gradient);
		summary.scorePValue = PDF.chi2(summary.scoreChiSquareValue, 1);

		DenseMatrix hessianInv = hessian.pseudoInverse();
		summary.waldChiSquareValue = new double[featureNum + 1];
		summary.waldPValues = new double[featureNum + 1];
		for (int i = 0; i < featureNum + 1; i++) {
			summary.waldChiSquareValue[i] = weights.get(i) * weights.get(i) / hessianInv.get(i, i);
			summary.waldPValues[i] = PDF.chi2(summary.waldChiSquareValue[i], 1);
		}

		summary.stdEsts = new double[featureNum + 1];
		summary.stdErrs = new double[featureNum + 1];
		summary.lowerConfidence = new double[featureNum + 1];
		summary.uperConfidence = new double[featureNum + 1];

		BaseVectorSummary dataSummary = srt.toSummary();
		for (int i = 0; i < featureNum + 1; i++) {
			summary.stdEsts[i] = dataSummary.standardDeviation(i) * summary.beta.get(i) / Math.sqrt(3) / Math.PI;
			summary.stdErrs[i] = Math.sqrt(hessianInv.get(i, i));
			summary.lowerConfidence[i] = summary.beta.get(i) - 1.96 * summary.stdErrs[i];
			summary.uperConfidence[i] = summary.beta.get(i) + 1.96 * summary.stdErrs[i];
		}

		summary.aic = 2 * summary.loss + 2 * (featureNum + 1);
		summary.sc = 2 * summary.loss + (featureNum + 1) * Math.log(dataSummary.count());

		return summary;
	}

	private static int[] indicesAddOne(int[] indices) {
		int[] result = new int[indices.length];
		Arrays.setAll(result, i -> indices[i] + 1);
		return result;
	}

	public static String getDefaultOptimMethod(String optimMethod, String constrained) {
		if (optimMethod == null || optimMethod.isEmpty()) {
			if (constrained == null || constrained.isEmpty()) {
				optimMethod = OptimMethod.LBFGS.name();
			} else {
				optimMethod = HasConstrainedOptimizationMethod.ConstOptimMethod.SQP.name();
			}
		}
		return optimMethod;
	}

	public static List <Tuple2 <Object, Vector>> predict(LinearModelData model, List <Vector> data) {
		LinearModelDataConverter converter = new LinearModelDataConverter(model.labelType);
		TableSchema modelSchema = converter.getModelSchema();
		TableSchema dataSchema = new TableSchema(new String[] {"features"}, new TypeInformation[] {AlinkTypes
			.VECTOR});

		LinearModelMapper mapper = new LinearModelMapper(
			modelSchema, dataSchema,
			new Params().set(LinearRegPredictParams.PREDICTION_COL, "pred"));
		mapper.loadModel(model);

		List <Tuple2 <Object, Vector>> result = new ArrayList <>();
		for (Vector vec : data) {
			try {
				if (model.hasInterceptItem) {
					result.add(Tuple2.of(mapper.predict(vec.prefix(1.0)), vec));
				} else {
					result.add(Tuple2.of(mapper.predict(vec), vec));
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		return result;
	}

}
