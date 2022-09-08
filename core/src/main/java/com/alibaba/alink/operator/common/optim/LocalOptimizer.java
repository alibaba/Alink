package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.params.regression.HasEpsilon;
import com.alibaba.alink.params.shared.HasNumCorrections_30;
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

	private static final int MAX_FEATURE_NUM = 1000;
	private static double EPS = 1.0e-18;

	/**
	 * autoCross function.
	 *
	 * @param objFunc   object function, calc loss and grad.
	 * @param trainData data for training.
	 * @param initCoef  the initial coefficient of problem.
	 * @param params    some parameters of optimization method.
	 */
	public static Tuple2 <DenseVector, Double> optimize(OptimObjFunc objFunc,
														List <Tuple3 <Double, Double, Vector>> trainData,
														DenseVector initCoef, Params params) throws Exception {
		LinearTrainParams.OptimMethod method = params.get(LinearTrainParams.OPTIM_METHOD);
		if (null == method) {
			if (params.get(HasL1.L_1) > 0) {
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

	public static Tuple2 <DenseVector, Double> gd(List <Tuple3 <Double, Double, Vector>> labledVectors,
												  DenseVector initCoefs,
												  Params params, OptimObjFunc objFunc) throws Exception {
		DenseVector coefs = initCoefs.clone();
		double epsilon = params.get(HasEpsilon.EPSILON);
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learningRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		int nCoef = initCoefs.size();
		DenseVector minVec = initCoefs.clone();
		double minLoss = objFunc.calcObjValue(labledVectors, coefs).f0;
		DenseVector grad = initCoefs.clone();
		for (int i = 0; i < maxIter; i++) {
			objFunc.calcGradient(labledVectors, coefs, grad);
			coefs = coefs.plus(grad.scale(-learningRate));
			double curLoss = objFunc.calcObjValue(labledVectors, coefs).f0;
			if (curLoss <= minLoss) {
				minLoss = curLoss;
				for (int j = 0; j < nCoef; ++j) {
					minVec.set(j, initCoefs.get(j));
				}
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"gd step (" + (i + 1) + ") "
						+ " current loss : " + curLoss + " and minLoss : " + minLoss);
			}
			if (curLoss < epsilon) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("gd converged at step : " + i);
				}
				break;
			}
		}
		return new Tuple2 <>(coefs, minLoss);
	}

	public static Tuple2 <DenseVector, Double> sgd(List <Tuple3 <Double, Double, Vector>> labledVectors,
												   DenseVector initCoefs,
												   Params params, OptimObjFunc objFunc) throws Exception {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learningRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);

		Random rand = new Random(2019);
		int nCoef = initCoefs.size();
		DenseVector minVec = initCoefs.clone();
		double minLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;
		DenseVector grad = new DenseVector(initCoefs.size());
		for (int i = 0; i < maxIter; i++) {
			for (int j = 0; j < labledVectors.size(); ++j) {
				Tuple3 <Double, Double, Vector> lb = labledVectors.get(rand.nextInt(labledVectors.size()));
				for (int k = 0; k < grad.size(); ++k) {
					grad.set(k, 0.0);
				}
				List <Tuple3 <Double, Double, Vector>> list = new ArrayList <>();
				list.add(lb);
				objFunc.calcGradient(list, initCoefs, grad);
				initCoefs.plusScaleEqual(grad, -learningRate);
			}

			double curLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;
			if (curLoss <= minLoss) {
				minLoss = curLoss;
				for (int j = 0; j < nCoef; ++j) {
					minVec.set(j, initCoefs.get(j));
				}
			}

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"sgd step (" + (i + 1) + ") "
						+ " current loss : " + curLoss + " and minLoss : " + minLoss);
			}
			if (curLoss < epsilon) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("sgd converged at step : " + i);
				}
				break;
			}
		}
		return new Tuple2 <>(initCoefs, minLoss);
	}

	public static Tuple2 <DenseVector, Double> newton(List <Tuple3 <Double, Double, Vector>> labledVectors,
													  DenseVector initCoefs, Params params, OptimObjFunc objFunc)
		throws Exception {
		Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> tuple4 =
			newtonWithHessian(labledVectors, initCoefs, params, objFunc);
		return Tuple2.of(tuple4.f0, tuple4.f3);
	}

	public static Tuple4 <DenseVector, DenseVector, DenseMatrix, Double> newtonWithHessian(
		List <Tuple3 <Double, Double, Vector>> labledVectors,
		DenseVector initCoefs, Params params, OptimObjFunc objFunc)
		throws Exception {
		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);
		int nCoef = initCoefs.size();
		if (!objFunc.hasSecondDerivative()) {
			throw new RuntimeException("the loss function doesn't have 2 order Derivative, newton can't work.");
		}
		if (nCoef > MAX_FEATURE_NUM) {
			throw new RuntimeException("Too many coefficients, newton can't work.");
		}

		DenseVector minVec = initCoefs.clone();
		double minLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;

		DenseVector gradSum = new DenseVector(nCoef);
		DenseMatrix hessianSum = new DenseMatrix(nCoef, nCoef);
		DenseVector t = new DenseVector(nCoef);
		DenseMatrix bMat = new DenseMatrix(nCoef, 1);
		for (int i = 0; i < maxIter; i++) {
			gradSum.scaleEqual(0.0);
			hessianSum.scaleEqual(0.0);
			Tuple2 <Double, Double>
				oldloss = objFunc.calcHessianGradientLoss(labledVectors, initCoefs, hessianSum, gradSum);
			for (int j = 0; j < nCoef; j++) {
				bMat.set(j, 0, gradSum.get(j));
			}
			DenseMatrix xMat = hessianSum.solveLS(bMat);
			for (int j = 0; j < nCoef; j++) {
				t.set(j, xMat.get(j, 0));
			}
			initCoefs.minusEqual(t);

			double curLoss = objFunc.calcObjValue(labledVectors, initCoefs).f0;

			if (curLoss <= minLoss) {
				minLoss = curLoss;
				for (int j = 0; j < nCoef; ++j) {
					minVec.set(j, initCoefs.get(j));
				}
			}
			double gradNorm = gradSum.scale(1 / oldloss.f0).normL2();
			double lossChangeRatio = Math.abs(curLoss - oldloss.f1 / oldloss.f0) / curLoss;
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(
					"********** local newton step (" + (i + 1) + ") current loss ratio : "
						+ lossChangeRatio + " and minLoss : " + minLoss + " grad : " + gradNorm);
			}
			if (gradNorm < epsilon || lossChangeRatio < epsilon) {
				break;
			}
		}
		return Tuple4.of(minVec, gradSum, hessianSum, minLoss);
	}

	/*
	 * fix the first several weight and only update the last some weight.
	 */
	public static Tuple2 <DenseVector, Double> lbfgsWithLast(List <Tuple3 <Double, Double, Vector>> labledVectors,
															 DenseVector initCoefs, Params params,
															 OptimObjFunc objFunc) {
		double[] fixedCoefs = params.get(FIXED_COEFS);
		Tuple2 <DenseVector, Double> optimizedCoef = lbfgsBase(labledVectors, initCoefs, params, objFunc, fixedCoefs);
		double[] optCoefData = optimizedCoef.f0.getData();
		double[] allCoefData = new double[fixedCoefs.length + optCoefData.length];
		System.arraycopy(fixedCoefs, 0, allCoefData, 0, fixedCoefs.length);
		System.arraycopy(optCoefData, 0, allCoefData, fixedCoefs.length, optCoefData.length);
		optimizedCoef.f0.setData(allCoefData);
		return optimizedCoef;
	}

	public static ParamInfo <double[]> FIXED_COEFS = ParamInfoFactory
		.createParamInfo("fixedCoefs", double[].class)
		.build();

	public static Tuple2 <DenseVector, Double> lbfgs(List <Tuple3 <Double, Double, Vector>> labledVectors,
													 DenseVector initCoefs, Params params, OptimObjFunc objFunc) {
		return lbfgsBase(labledVectors, initCoefs, params, objFunc, new double[0]);
	}

	//input initCoefs is candidate coefs, while fixedCoefs saved. they are all onehoted.
	private static Tuple2 <DenseVector, Double> lbfgsBase(List <Tuple3 <Double, Double, Vector>> labledVectors,
														  DenseVector initCoefs, Params params, OptimObjFunc objFunc,
														  double[] fixedCoefs) {
		DenseVector allCoefs = new DenseVector(initCoefs.size() + fixedCoefs.length);
		System.arraycopy(initCoefs.getData(), 0, allCoefs.getData(), fixedCoefs.length, initCoefs.size());
		System.arraycopy(fixedCoefs, 0, allCoefs.getData(), 0, fixedCoefs.length);

		int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
		double learnRate = params.get(HasLearningRateDefaultAs01.LEARNING_RATE);
		double epsilon = params.get(HasEpsilonDefaultAs0000001.EPSILON);
		int nCoef = initCoefs.size();
		int numCorrection = params.get(HasNumCorrections_30.NUM_CORRECTIONS);

		DenseVector[] yK = new DenseVector[numCorrection];
		DenseVector[] sK = new DenseVector[numCorrection];
		for (int i = 0; i < numCorrection; ++i) {
			yK[i] = new DenseVector(initCoefs.size());
			sK[i] = new DenseVector(initCoefs.size());
		}
		DenseVector oldGradient = new DenseVector(initCoefs.size());
		DenseVector minVec = initCoefs.clone();
		DenseVector dir = null;
		double[] alpha = new double[numCorrection];

		double minLoss = objFunc.calcObjValue(labledVectors, allCoefs).f0;

		double stepLength = -1.0;
		DenseVector gradient = initCoefs.clone();
		for (int i = 0; i < maxIter; i++) {
			double weightSum = objFunc.calcGradient(labledVectors, allCoefs, gradient);

			if (i == 0) {
				dir = gradient.clone();
			}
			dir = calcDir(yK, sK, numCorrection, gradient, oldGradient, i, alpha, stepLength, dir);

			int numSearchStep = 10;

			double beta = learnRate / numSearchStep;
			double[] losses = objFunc.calcSearchValues(labledVectors, allCoefs, dir, beta, numSearchStep);
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

			initCoefs.minusEqual(dir.scale(stepLength));
			System.arraycopy(initCoefs.getData(), 0, allCoefs.getData(), fixedCoefs.length, initCoefs.size());

			if (losses[0] / weightSum <= minLoss) {
				minLoss = losses[0] / weightSum;
				for (int j = 0; j < nCoef; ++j) {
					minVec.set(j, initCoefs.get(j));
				}
			}
			double gradNorm = Math.sqrt(gradient.normL2Square());

			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println("LBFGS step (" + (i + 1) + ") learnRate : " + learnRate
					+ " current loss : " + losses[0] / weightSum + " and minLoss : " + minLoss + " grad norm : "
					+ gradNorm);
			}
			if (gradNorm < epsilon || learnRate < EPS) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("LBFGS converged at step : " + i);
				}
				break;
			}
		}
		return new Tuple2 <>(minVec, minLoss);
	}
	//        DenseVector allCoefs = new DenseVector(initCoefs.size()+fixedCoefs.length);
	//        System.arraycopy(initCoefs.getData(), 0, allCoefs.getData(), 0, initCoefs.size());
	//        System.arraycopy(fixedCoefs, 0, allCoefs.getData(), initCoefs.size(), fixedCoefs.length);
	//        DenseVector gradient = initCoefs.clone();
	//
	//        int maxIter = params.get(HasMaxIterDefaultAs100.MAX_ITER).intValue();
	//        double learnRate = params.get(HasLearningRateDv01.LEARNING_RATE);
	//        double epsilon = params.get(HasEpsilonDv0000001.EPSILON);
	//        int nCoef = initCoefs.size();
	//        int numCorrection = params.get(HasNumCorrections_30.NUM_CORRECTIONS);
	//
	//        DenseVector[] yK = new DenseVector[numCorrection];
	//        DenseVector[] sK = new DenseVector[numCorrection];
	//        for (int i = 0; i < numCorrection; ++i) {
	//            yK[i] = new DenseVector(initCoefs.size());
	//            sK[i] = new DenseVector(initCoefs.size());
	//        }
	//        DenseVector oldGradient = new DenseVector(initCoefs.size());
	//        DenseVector minVec = initCoefs.clone();
	//        DenseVector dir = null;
	//        double[] alpha = new double[numCorrection];
	//
	//        double minLoss = objFunc.calcObjValue(labledVectors, allCoefs).f0;
	//
	//        double stepLength = -1.0;
	//
	//        for (int i = 0; i < maxIter; i++) {
	//            double weightSum = objFunc.calcGradient(labledVectors, allCoefs, gradient);
	//
	//            if (i == 0) {
	//                dir = gradient.clone();
	//            }
	//            dir = calcDir(yK, sK, numCorrection, gradient, oldGradient, i, alpha, stepLength, dir);
	//
	//            int numSearchStep = 10;
	//
	//            double beta = learnRate / numSearchStep;
	//            double[] losses = objFunc.calcSearchValues(labledVectors, allCoefs, dir, beta, numSearchStep);
	//            int pos = -1;
	//            for (int j = 1; j < losses.length; ++j) {
	//                if (losses[j] < losses[0]) {
	//                    losses[0] = losses[j];
	//                    pos = j;
	//                }
	//            }
	//
	//            if (pos == -1) {
	//                stepLength = 0.0;
	//                learnRate *= 0.1;
	//            } else if (pos == 10) {
	//                stepLength = beta * pos;
	//                learnRate *= 10.0;
	//            } else {
	//                stepLength = beta * pos;
	//            }
	//
	//            updateFunc.updateModel(initCoefs, null, dir, stepLength);
	//
	//            if (losses[0] / weightSum <= minLoss) {
	//                minLoss = losses[0] / weightSum;
	//                for (int j = 0; j < nCoef; ++j) {
	//                    minVec.set(j, initCoefs.get(j));
	//                }
	//            }
	//            double gradNorm = Math.sqrt(gradient.normL2Square());
	//
	//            if (GlobalConfiguration.isPrintProcessInfo()) {
	//                System.out.println("LBFGS step (" + (i + 1) + ") learnRate : " + learnRate
	//                    + " current loss : " + losses[0] / weightSum + " and minLoss : " + minLoss + " grad norm : "
	//                    + gradNorm);
	//            }
	//            if (gradNorm < epsilon || learnRate < EPS) {
	//                if (GlobalConfiguration.isPrintProcessInfo()) {
	//                    System.out.println("LBFGS converged at step : " + i);
	//                }
	//                break;
	//            }
	//        }
	//        return new Tuple2<>(minVec, minLoss);
	//    }

	public static Tuple2 <DenseVector, Double> owlqn(List <Tuple3 <Double, Double, Vector>> labledVectors,
													 DenseVector initCoefs, Params params, OptimObjFunc objFunc)
		throws Exception {
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
		for (int i = 0; i < maxIter; i++) {
			double weightSum = objFunc.calcGradient(labledVectors, initCoefs, gradient);

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
			double[] losses = objFunc.constraintCalcSearchValues(labledVectors, initCoefs, dir, beta, numSearchStep);

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
			if (gradNorm < epsilon || learnRate < EPS) {
				if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
					System.out.println("owlqn converged at step : " + i);
				}
				break;
			}
		}

		return new Tuple2 <>(minVec, minLoss);
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
											double[] alpha) throws Exception {
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
}
