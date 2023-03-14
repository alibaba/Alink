package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.operator.local.AlinkLocalSession.TaskRunner;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.regression.HasEpsilon;
import com.alibaba.alink.params.shared.HasNumCorrections_30;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams.OptimMethod;
import com.alibaba.alink.params.shared.optim.HasLearningRateDefaultAs01;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * optimizers in this class are serial, which solve the problem with one worker.
 *
 * @author yangxu
 */
public class LocalOptimizer {

	private static final int NEWTON_MAX_FEATURE_NUM = 1024;
	private static final double EPS = 1.0e-18;

	/**
	 * autoCross function.
	 *
	 * @param objFunc   object function, calc loss and grad.
	 * @param trainData data for training.
	 * @param initCoef  the initial coefficient of problem.
	 * @param params    some parameters of optimization method.
	 */
	public static Tuple2 <DenseVector, double[]> optimize(OptimObjFunc objFunc,
														  List <Tuple3 <Double, Double, Vector>> trainData,
														  DenseVector initCoef, Params params) {
		LinearTrainParams.OptimMethod method = params.get(LinearTrainParams.OPTIM_METHOD);
		if (null == method) {
			if (trainData.get(0).f2.size() <= NEWTON_MAX_FEATURE_NUM) {
				method = OptimMethod.Newton;
			} else if (params.get(HasL1.L_1) > 0) {
				method = OptimMethod.OWLQN;
			} else {
				method = OptimMethod.LBFGS;
			}
		}

		switch (method) {
			case GD:
				return gd(trainData, initCoef, params, objFunc);
			case SGD:
				return sgd(trainData, initCoef, params, objFunc);
			case Newton:
				return newton(trainData, initCoef, params, objFunc);
			case LBFGS:
				return lbfgs(trainData, initCoef, params, objFunc);
			case OWLQN:
				return owlqn(trainData, initCoef, params, objFunc);
			default:
				throw new RuntimeException("the method: " + method + " is not exists.");
		}
	}

	public static int getNumThreads(List <Tuple3 <Double, Double, Vector>> labledVectors, Params params) {
		int numThreads = LocalOperator.getDefaultNumThreads();
		if (params.contains(HasNumThreads.NUM_THREADS)) {
			numThreads = params.get(HasNumThreads.NUM_THREADS);
		}
		return Math.min(numThreads, labledVectors.size());
	}

	public static double[] getFinalConvergeInfos(double[] convergeInfos, int curIter) {
		int n = 3 * (curIter + 1);
		double[] finalInfos = new double[n];
		System.arraycopy(convergeInfos, 0, finalInfos, 0, n);
		return finalInfos;
	}

	public static Tuple2 <DenseVector, double[]> gd(List <Tuple3 <Double, Double, Vector>> labledVectors,
													DenseVector initCoefs,
													Params params, OptimObjFunc objFunc) {
		DenseVector coefs = initCoefs.clone();
		double epsilon = params.get(HasEpsilon.EPSILON);
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learningRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		int nCoef = initCoefs.size();
		DenseVector minVec = initCoefs.clone();
		double minLoss = objFunc.calcObjValue(labledVectors, coefs).f0;
		DenseVector grad = initCoefs.clone();
		double[] convergeInfos = new double[3 * maxIter];
		double lastLoss = Double.POSITIVE_INFINITY;
		for (int i = 0; i < maxIter; i++) {
			objFunc.calcGradient(labledVectors, coefs, grad);
			coefs = coefs.plus(grad.scale(-learningRate));
			double curLoss = objFunc.calcObjValue(labledVectors, coefs).f0;
			if (curLoss <= minLoss) {
				minLoss = curLoss;
				minVec.setEqual(initCoefs);
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"gd step (" + (i + 1) + ") "
						+ " current loss : " + curLoss + " and minLoss : " + minLoss);
			}
			convergeInfos[3 * i] = curLoss;
			convergeInfos[3 * i + 1] = grad.normL2();
			convergeInfos[3 * i + 2] = learningRate;
			if (curLoss < epsilon || Math.abs(lastLoss - curLoss) / curLoss < epsilon) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("gd converged at step : " + i);
				}
				convergeInfos = getFinalConvergeInfos(convergeInfos, i);
				break;
			}
			lastLoss = curLoss;
		}
		return new Tuple2 <>(coefs, convergeInfos);
	}

	public static Tuple2 <DenseVector, double[]> sgd(List <Tuple3 <Double, Double, Vector>> labledVectors,
													 DenseVector initCoefs,
													 Params params, OptimObjFunc objFunc) {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learningRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);

		Random rand = new Random(2019);
		int nCoef = initCoefs.size();
		DenseVector minVec = initCoefs.clone();
		double minLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;
		DenseVector grad = new DenseVector(initCoefs.size());
		double[] convergeInfos = new double[3 * maxIter];
		double lastLoss = Double.POSITIVE_INFINITY;
		for (int i = 0; i < maxIter; i++) {
			for (int j = 0; j < labledVectors.size(); ++j) {
				Tuple3 <Double, Double, Vector> lb = labledVectors.get(rand.nextInt(labledVectors.size()));
				List <Tuple3 <Double, Double, Vector>> list = new ArrayList <>();
				list.add(lb);
				objFunc.calcGradient(list, initCoefs, grad);
				initCoefs.plusScaleEqual(grad, -learningRate);
			}
			Double curLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;

			//DenseVector oldCoef = initCoefs.clone();
			//for (int j = 0; j < labledVectors.size(); ++j) {
			//	Tuple3 <Double, Double, Vector> lb = labledVectors.get(rand.nextInt(labledVectors.size()));
			//	//Arrays.fill(grad.getData(), 0.0);
			//	List <Tuple3 <Double, Double, Vector>> list = new ArrayList <>();
			//	list.add(lb);
			//	objFunc.calcGradient(list, initCoefs, grad);
			//	initCoefs.plusScaleEqual(grad, -learningRate);
			//}
			//
			//Double curLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;
			//if (curLoss.isNaN() || curLoss.isInfinite()) {
			//	initCoefs = oldCoef;
			//	learningRate *= 0.25;
			//	System.out.println("learning rate changed from " + learningRate + " to " + learningRate * 0.5);
			//	i--;
			//	continue;
			//}

			if (curLoss <= minLoss) {
				minLoss = curLoss;
				minVec.setEqual(initCoefs);
			}
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"sgd step (" + (i + 1) + ") "
						+ " current loss : " + curLoss + " and minLoss : " + minLoss);
			}
			convergeInfos[3 * i] = curLoss;
			convergeInfos[3 * i + 1] = grad.normL2();
			convergeInfos[3 * i + 2] = learningRate;
			if (curLoss < epsilon || Math.abs(lastLoss - curLoss) / curLoss < epsilon) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("sgd converged at step : " + i);
				}
				convergeInfos = getFinalConvergeInfos(convergeInfos, i);
				break;
			}
			lastLoss = curLoss;
		}
		return new Tuple2 <>(initCoefs, convergeInfos);
	}

	public static Tuple2 <DenseVector, double[]> newton(List <Tuple3 <Double, Double, Vector>> labledVectors,
														DenseVector initCoefs, Params params, OptimObjFunc objFunc) {
		Tuple4 <DenseVector, DenseVector, DenseMatrix, double[]> tuple4 =
			newtonWithHessian(labledVectors, initCoefs, params, objFunc);
		return Tuple2.of(tuple4.f0, tuple4.f3);
	}

	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, double[]> newtonWithHessian(
		List <Tuple3 <Double, Double, Vector>> labledVectors,
		DenseVector initCoefs, Params params, OptimObjFunc objFunc) {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);
		int nCoef = initCoefs.size();
		if (!objFunc.hasSecondDerivative()) {
			throw new RuntimeException("the loss function doesn't have 2 order Derivative, newton can't work.");
		}
		if (nCoef > NEWTON_MAX_FEATURE_NUM) {
			throw new RuntimeException("Too many coefficients, newton can't work.");
		}

		int numThreads = getNumThreads(labledVectors, params);

		DenseVector minVec = initCoefs.clone();
		double minLoss = calcObjValueMT(objFunc, numThreads, labledVectors, initCoefs).f0;

		DenseVector gradSum = new DenseVector(nCoef);
		DenseMatrix hessianSum = new DenseMatrix(nCoef, nCoef);
		DenseVector t = new DenseVector(nCoef);
		DenseMatrix bMat = new DenseMatrix(nCoef, 1);
		double[] convergeInfos = new double[3 * maxIter];
		for (int i = 0; i < maxIter; i++) {
			Tuple2 <Double, Double> oldloss
				= calcHessianGradientLossMT(objFunc, numThreads, labledVectors, initCoefs, hessianSum, gradSum);
			for (int j = 0; j < nCoef; j++) {
				bMat.set(j, 0, gradSum.get(j));
			}
			DenseMatrix xMat = hessianSum.solveLS(bMat);
			for (int j = 0; j < nCoef; j++) {
				t.set(j, xMat.get(j, 0));
			}
			initCoefs.minusEqual(t);

			double curLoss = calcObjValueMT(objFunc, numThreads, labledVectors, initCoefs).f0;

			if (curLoss <= minLoss) {
				minLoss = curLoss;
				minVec.setEqual(initCoefs);
			}
			double gradNorm = gradSum.scale(1 / oldloss.f0).normL2();
			double lossChangeRatio = Math.abs(curLoss - oldloss.f1 / oldloss.f0) / curLoss;
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"********** local newton step (" + (i + 1) + ") current loss ratio : "
						+ lossChangeRatio + " and minLoss : " + minLoss + " grad : " + gradNorm);
			}
			convergeInfos[3 * i] = curLoss;
			convergeInfos[3 * i + 1] = gradNorm;
			convergeInfos[3 * i + 2] = Double.NaN;
			if (gradNorm < epsilon || lossChangeRatio < epsilon) {
				convergeInfos = getFinalConvergeInfos(convergeInfos, i);
				break;
			}
		}
		return Tuple4.of(minVec, gradSum, hessianSum, convergeInfos);
	}

	public static Tuple2 <DenseVector, double[]> lbfgs(List <Tuple3 <Double, Double, Vector>> labledVectors,
													   DenseVector initCoefs, Params params, OptimObjFunc objFunc) {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER);
		double learnRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);
		int numCorrection = params.get(HasNumCorrections_30.NUM_CORRECTIONS);
		int nCoef = initCoefs.size();

		DenseVector[] yK = new DenseVector[numCorrection];
		DenseVector[] sK = new DenseVector[numCorrection];
		for (int i = 0; i < numCorrection; ++i) {
			yK[i] = new DenseVector(nCoef);
			sK[i] = new DenseVector(nCoef);
		}
		DenseVector oldGradient = new DenseVector(nCoef);
		DenseVector minVec = initCoefs.clone();
		DenseVector dir = null;
		double[] alpha = new double[numCorrection];

		double minLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;

		double stepLength = -1.0;
		DenseVector gradient = initCoefs.clone();
		double[] convergeInfos = new double[3 * maxIter];

		int numThreads = getNumThreads(labledVectors, params);

		DenseVector coefs = initCoefs.clone();
		for (int i = 0; i < maxIter; i++) {
			double weightSum = calcGradientMT(objFunc, numThreads, labledVectors, coefs, gradient);

			if (i == 0) {
				dir = gradient.clone();
			}
			dir = calcDir(yK, sK, numCorrection, gradient, oldGradient, i, alpha, stepLength, dir);

			int numSearchStep = 10;

			double beta = learnRate / numSearchStep;

			double[] losses = calcSearchValuesMT(objFunc, numThreads, labledVectors, coefs, dir, beta, numSearchStep);

			int pos = -1;
			for (int j = 1; j < losses.length; ++j) {
				if (losses[j] < losses[0]) {
					losses[0] = losses[j];
					pos = j;
				}
			}

			if (pos == -1) {
				stepLength = 0.0;
				learnRate *= 0.1;
			} else if (pos == 10) {
				stepLength = beta * pos;
				learnRate *= 10.0;
			} else {
				stepLength = beta * pos;
			}

			coefs.minusEqual(dir.scale(stepLength));

			if (losses[0] / weightSum <= minLoss) {
				minLoss = losses[0] / weightSum;
				minVec.setEqual(coefs);
			}
			double gradNorm = Math.sqrt(gradient.normL2Square());

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("LBFGS step (" + (i + 1) + ") learnRate : " + learnRate
					+ " current loss : " + losses[0] / weightSum + " and minLoss : " + minLoss + " grad norm : "
					+ gradNorm);
			}
			convergeInfos[3 * i] = losses[0] / weightSum;
			convergeInfos[3 * i + 1] = gradNorm;
			convergeInfos[3 * i + 2] = learnRate;
			if (gradNorm < epsilon || learnRate < EPS) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("LBFGS converged at step : " + i);
				}
				convergeInfos = getFinalConvergeInfos(convergeInfos, i);
				break;
			}
		}
		return new Tuple2 <>(minVec, convergeInfos);
	}

	public static Tuple2 <DenseVector, double[]> owlqn(List <Tuple3 <Double, Double, Vector>> labledVectors,
													   DenseVector initCoefs, Params params, OptimObjFunc objFunc) {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learnRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);
		int nCoef = initCoefs.size();
		int numCorrection = params.get(HasNumCorrections_30.NUM_CORRECTIONS);

		DenseVector[] yK = new DenseVector[numCorrection];
		DenseVector[] sK = new DenseVector[numCorrection];
		for (int i = 0; i < numCorrection; ++i) {
			yK[i] = new DenseVector(nCoef);
			sK[i] = new DenseVector(nCoef);
		}
		DenseVector oldGradient = new DenseVector(nCoef);
		DenseVector pseGradient = new DenseVector(nCoef);
		DenseVector minVec = initCoefs.clone();
		DenseVector dir;
		double[] alpha = new double[numCorrection];
		double minLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;
		double stepLength;
		DenseVector gradient = initCoefs.clone();
		double[] convergeInfos = new double[3 * maxIter];

		int numThreads = getNumThreads(labledVectors, params);

		for (int i = 0; i < maxIter; i++) {
			double weightSum = calcGradientMT(objFunc, numThreads, labledVectors, initCoefs, gradient);

			if (objFunc.getL1() > 0.0) {
				/** transfer gradient to pseudo-gradient */
				for (int m = 0; m < nCoef; ++m) {
					if (initCoefs.get(m) == 0) {
						if (gradient.get(m) + objFunc.getL1() < 0) {
							pseGradient.set(m, gradient.get(m) + objFunc.getL1());
						} else if (gradient.get(m) - objFunc.getL1() > 0) {
							pseGradient.set(m, gradient.get(m) - objFunc.getL1());
						} else {
							pseGradient.set(m, 0.0);
						}
					} else {
						pseGradient.set(m, gradient.get(m));
					}
				}
			} else {
				for (int m = 0; m < nCoef; ++m) {
					pseGradient.set(m, gradient.get(m));
				}
			}

			dir = calcOwlqnDir(yK, sK, numCorrection, gradient, oldGradient, pseGradient, i, alpha);

			/** dir ⇐ project(dir, gradient) */
			if (objFunc.getL1() > 0.0) {
				for (int s = 0; s < nCoef; ++s) {
					if (dir.get(s) * pseGradient.get(s) < 0) {
						dir.set(s, 0.0);
					}
				}
			}
			int numSearchStep = 10;

			double beta = learnRate / numSearchStep;
			double[] losses = constraintCalcSearchValuesMT(objFunc, numThreads, labledVectors, initCoefs, dir, beta,
				numSearchStep);

			double curLoss;
			int pos = -1;
			for (int j = 1; j < losses.length; ++j) {
				if (losses[j] < losses[0]) {
					losses[0] = losses[j];
					pos = j;
				}
			}

			if (pos == -1) {
				stepLength = 0.0;
				learnRate *= 0.1;
				curLoss = losses[0] / weightSum;
			} else if (pos == 10) {
				stepLength = beta * pos;
				learnRate *= 10.0;
				curLoss = losses[pos] / weightSum;
			} else {
				stepLength = beta * pos;
				curLoss = losses[pos] / weightSum;
			}

			/* constrained of the linear search */
			if (objFunc.getL1() > 0.0) {
				for (int s = 0; s < dir.size(); ++s) {
					double val = initCoefs.get(s);
					double newVal = initCoefs.get(s) - dir.get(s) * stepLength;
					if (Math.abs(initCoefs.get(s)) > 0) {
						if (newVal * val < 0) {
							initCoefs.set(s, 0.0);
							sK[i % numCorrection].set(s, -val);
						} else {
							sK[i % numCorrection].set(s, -dir.get(s) * stepLength);
							initCoefs.set(s, newVal);
						}
					} else if (newVal * pseGradient.get(s) > 0) {
						initCoefs.set(s, 0.0);
						sK[i % numCorrection].set(s, 0.0);
					} else {
						sK[i % numCorrection].set(s, -dir.get(s) * stepLength);
						initCoefs.set(s, newVal);
					}
				}
			} else {
				for (int s = 0; s < dir.size(); ++s) {
					sK[i % numCorrection].set(s, -dir.get(s) * stepLength);
					initCoefs.set(s, initCoefs.get(s) - dir.get(s) * stepLength);
				}
			}

			if (curLoss <= minLoss) {
				minLoss = curLoss;
				minVec.setEqual(initCoefs);
			}
			double gradNorm = Math.sqrt(gradient.normL2Square());
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"owlqn step (" + (i + 1) + ") learnRate : " + learnRate
						+ " current loss : " + curLoss + " and minLoss : " + minLoss + " gradNorm : " + gradNorm);
			}
			convergeInfos[3 * i] = curLoss;
			convergeInfos[3 * i + 1] = gradNorm;
			convergeInfos[3 * i + 2] = learnRate;
			if (gradNorm < epsilon || learnRate < EPS) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("owlqn converged at step : " + i);
				}
				convergeInfos = getFinalConvergeInfos(convergeInfos, i);
				break;
			}
		}

		return new Tuple2 <>(minVec, convergeInfos);
	}

	private static DenseVector calcDir(DenseVector[] yK, DenseVector[] sK, int m, DenseVector gradient,
									   DenseVector oldGradient, int k, double[] alpha, double stepLength,
									   DenseVector dir) {

		DenseVector qL = gradient.clone();

		// update Y_k = g_k+1 - g_k
		if (k > 0) {
			for (int s = 0; s < gradient.size(); ++s) {
				sK[(k - 1) % m].set(s, dir.get(s) * (-stepLength));
			}
		}
		DenseVector vec = gradient;
		if (k == 0) {
			for (int s = 0; s < gradient.size(); ++s) {
				qL.set(s, vec.get(s));
			}
			oldGradient.setEqual(vec);
		} else {
			yK[(k - 1) % m].setEqual(qL);
			yK[(k - 1) % m].minusEqual(oldGradient);
			oldGradient.setEqual(vec);
		}

		// compute Hr^-1 * g_k
		int delta = k > m ? k - m : 0;
		int l = Math.min(k, m);
		if (alpha == null) {
			alpha = new double[m];
		}
		for (int i = l - 1; i >= 0; i--) {
			int j = (i + delta) % m;
			double dot = sK[j].dot(yK[j]);
			if (Math.abs(dot) > 0) {
				double rhoJ = 1.0 / dot;
				alpha[i] = rhoJ * (sK[j].dot(qL));

				qL.plusScaleEqual(yK[j], -alpha[i]);
			}
		}

		for (int i = 0; i < l; i++) {
			int j = (i + delta) % m;
			double dot = sK[j].dot(yK[j]);
			if (Math.abs(dot) > 0) {
				double rhoJ = 1.0 / dot;
				double betaI = rhoJ * (yK[j].dot(qL));
				qL.plusScaleEqual(sK[j], (alpha[i] - betaI));
			}
		}
		return qL;
	}

	private static DenseVector calcOwlqnDir(DenseVector[] yK, DenseVector[] sK, int m, DenseVector gradient,
											DenseVector oldGradient, DenseVector pseGradient, int k,
											double[] alpha) {
		DenseVector qL = pseGradient.clone();
		// update Y_k = g_k+1 - g_k
		if (k == 0) {
			oldGradient.setEqual(gradient);
		} else {
			yK[(k - 1) % m].setEqual(gradient);
			yK[(k - 1) % m].minusEqual(oldGradient);
			oldGradient.setEqual(gradient);

			// compute Hr^-1 * g_k
			int delta = k > m ? k - m : 0;
			int l = k <= m ? k : m;
			if (alpha == null) {
				alpha = new double[m];
			}
			for (int i = l - 1; i >= 0; i--) {
				int j = (i + delta) % m;
				double dot = sK[j].dot(yK[j]);
				if (Math.abs(dot) > 0) {
					double rhoJ = 1.0 / dot;
					alpha[i] = rhoJ * (sK[j].dot(qL));

					qL.plusScaleEqual(yK[j], -alpha[i]);
				}
			}

			for (int i = 0; i < l; i++) {
				int j = (i + delta) % m;
				double dot = sK[j].dot(yK[j]);
				if (Math.abs(dot) > 0) {
					double rhoJ = 1.0 / dot;
					double betaI = rhoJ * (yK[j].dot(qL));
					qL.plusScaleEqual(sK[j], (alpha[i] - betaI));
				}
			}
		}
		return qL;
	}

	private static Tuple2 <Double, Double> calcObjValueMT(OptimObjFunc objFunc, int numThreads,
														  List <Tuple3 <Double, Double, Vector>> labelVectors,
														  DenseVector coefVector) {
		if (numThreads <= 1) {
			return objFunc.calcObjValue(labelVectors, coefVector);
		} else {
			TaskRunner taskRunner = new TaskRunner();
			final SubIterator <Tuple3 <Double, Double, Vector>>[] subIterators = new SubIterator[numThreads];
			final double[] outWeightSum = new double[numThreads];
			final double[] outFVal = new double[numThreads];
			for (int k = 0; k < numThreads; k++) {
				subIterators[k] = new SubIterator <>(labelVectors, numThreads, k);
			}
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						double weightSum = 0.0;
						double fVal = 0.0;
						double loss;
						for (Tuple3 <Double, Double, Vector> labelVector : subIterators[curThread]) {
							loss = objFunc.calcLoss(labelVector, coefVector);
							fVal += loss * labelVector.f0;
							weightSum += labelVector.f0;
						}
						outFVal[curThread] = fVal;
						outWeightSum[curThread] = weightSum;
					}
				);
			}
			taskRunner.join();
			double weightSum = outWeightSum[0];
			double fVal = outFVal[0];
			for (int k = 1; k < numThreads; k++) {
				weightSum += outWeightSum[k];
				fVal += outFVal[k];
			}
			return objFunc.finalizeObjValue(coefVector, fVal, weightSum);
		}
	}

	private static Tuple2 <Double, Double> calcHessianGradientLossMT(OptimObjFunc objFunc, int numThreads,
																	 List <Tuple3 <Double, Double, Vector>> labelVectors,
																	 DenseVector coefVector,
																	 DenseMatrix hessian,
																	 DenseVector grad) {
		if (numThreads <= 1) {
			return objFunc.calcHessianGradientLoss(labelVectors, coefVector, hessian, grad);
		} else {
			if (!objFunc.hasSecondDerivative()) {
				throw new AkUnsupportedOperationException(
					"loss function can't support second derivative, newton precondition can not work.");
			}

			TaskRunner taskRunner = new TaskRunner();
			final SubIterator <Tuple3 <Double, Double, Vector>>[] subIterators = new SubIterator[numThreads];
			final double[] outWeightSum = new double[numThreads];
			final double[] outLoss = new double[numThreads];
			int nCoefs = coefVector.size();
			final DenseVector[] subGrads = new DenseVector[numThreads];
			final DenseMatrix[] subHessian = new DenseMatrix[numThreads];
			for (int k = 0; k < numThreads; k++) {
				subIterators[k] = new SubIterator <>(labelVectors, numThreads, k);
				subGrads[k] = new DenseVector(nCoefs);
				subHessian[k] = new DenseMatrix(nCoefs, nCoefs);
			}
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						double weightSum = 0.0;
						double loss = 0.0;
						for (Tuple3 <Double, Double, Vector> labelVector : subIterators[curThread]) {
							loss = objFunc.calcLoss(labelVector, coefVector);
							weightSum += labelVector.f0;
							objFunc.updateGradient(labelVector, coefVector, subGrads[curThread]);
							objFunc.updateHessian(labelVector, coefVector, subHessian[curThread]);
						}
						outLoss[curThread] = loss;
						outWeightSum[curThread] = weightSum;
					}
				);
			}
			taskRunner.join();

			double weightSum = outWeightSum[0];
			double loss = outLoss[0];
			grad.setEqual(subGrads[0]);
			System.arraycopy(subHessian[0].getData(), 0, hessian.getData(), 0, nCoefs * nCoefs);
			for (int k = 1; k < numThreads; k++) {
				weightSum += outWeightSum[k];
				loss += outLoss[k];
				grad.plusEqual(subGrads[k]);
				hessian.plusEquals(subHessian[k]);
			}

			objFunc.finalizeHessianGradientLoss(coefVector, hessian, grad, weightSum);

			return Tuple2.of(weightSum, loss);
		}
	}

	private static double calcGradientMT(OptimObjFunc objFunc, int numThreads,
										 List <Tuple3 <Double, Double, Vector>> labelVectors,
										 DenseVector coefVector, DenseVector grad) {
		if (numThreads <= 1) {
			return objFunc.calcGradient(labelVectors, coefVector, grad);
		} else {
			int nCoefs = coefVector.size();
			TaskRunner taskRunner = new TaskRunner();
			final SubIterator <Tuple3 <Double, Double, Vector>>[] subIterators = new SubIterator[numThreads];
			final double[] outValues = new double[numThreads];
			final DenseVector[] subGrads = new DenseVector[numThreads];
			for (int k = 0; k < numThreads; k++) {
				subIterators[k] = new SubIterator <>(labelVectors, numThreads, k);
				subGrads[k] = new DenseVector(nCoefs);
			}
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						for (Tuple3 <Double, Double, Vector> labelVector : subIterators[curThread]) {
							outValues[curThread] += labelVector.f0;
							objFunc.updateGradient(labelVector, coefVector, subGrads[curThread]);
						}
					}
				);
			}
			taskRunner.join();
			double weightSum = outValues[0];
			grad.setEqual(subGrads[0]);
			for (int k = 1; k < numThreads; k++) {
				weightSum += outValues[k];
				grad.plusEqual(subGrads[k]);
			}
			objFunc.finalizeGradient(coefVector, grad, weightSum);

			return weightSum;
		}
	}

	private static double[] calcSearchValuesMT(final OptimObjFunc objFunc, int numThreads,
											   List <Tuple3 <Double, Double, Vector>> labelVectors,
											   DenseVector coefVector,
											   DenseVector dirVec, double beta, int numStep) {
		if (numThreads <= 1) {
			return objFunc.calcSearchValues(labelVectors, coefVector, dirVec, beta, numStep);
		} else {
			TaskRunner taskRunner = new TaskRunner();
			final SubIterator <Tuple3 <Double, Double, Vector>>[] subIterators = new SubIterator[numThreads];
			final double[][] lossMat = new double[numThreads][numStep + 1];
			for (int k = 0; k < numThreads; k++) {
				subIterators[k] = new SubIterator <>(labelVectors, numThreads, k);
			}
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						double[] sublosses = objFunc.calcSearchValues(subIterators[curThread], coefVector, dirVec,
							beta,
							numStep);
						System.arraycopy(sublosses, 0, lossMat[curThread], 0, numStep + 1);
					}
				);
			}
			taskRunner.join();
			double[] losses = lossMat[0];
			for (int j = 1; j < numThreads; j++) {
				for (int jj = 0; jj <= numStep; jj++) {
					losses[jj] += lossMat[j][jj];
				}
			}
			return losses;
		}
	}

	private static double[] constraintCalcSearchValuesMT(OptimObjFunc objFunc, int numThreads,
														 List <Tuple3 <Double, Double, Vector>> labelVectors,
														 DenseVector coefVector,
														 DenseVector dirVec, double beta, int numStep) {
		if (numThreads <= 1) {
			return objFunc.constraintCalcSearchValues(labelVectors, coefVector, dirVec, beta, numStep);
		} else {
			TaskRunner taskRunner = new TaskRunner();
			final List <Tuple3 <Double, Double, Vector>>[] subLists = new List[numThreads];
			final double[][] lossMat = new double[numThreads][numStep + 1];
			int nTotal = labelVectors.size();
			int nSub = nTotal / numThreads;
			for (int k = 0; k < numThreads - 1; k++) {
				subLists[k] = labelVectors.subList(nSub * k, nSub * (k + 1));
			}
			subLists[numThreads - 1] = labelVectors.subList(nSub * (numThreads - 1), nTotal);
			for (int k = 0; k < numThreads; k++) {
				final int curThread = k;
				taskRunner.submit(() -> {
						double[] sublosses = objFunc.constraintCalcSearchValues(subLists[curThread], coefVector,
							dirVec, beta, numStep);
						System.arraycopy(sublosses, 0, lossMat[curThread], 0, numStep + 1);
					}
				);
			}
			taskRunner.join();
			double[] losses = lossMat[0];
			for (int j = 1; j < numThreads; j++) {
				for (int jj = 0; jj <= numStep; jj++) {
					losses[jj] += lossMat[j][jj];
				}
			}
			return losses;
		}
	}

}
