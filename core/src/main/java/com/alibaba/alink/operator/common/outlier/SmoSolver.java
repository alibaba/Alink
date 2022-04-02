package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.AlinkGlobalConfiguration;

public class SmoSolver {
	private int activeSize;
	private final byte[] y;
	private final double[] gradient;
	private static final byte LOWER_BOUND = 0;
	private static final byte UPPER_BOUND = 1;
	private static final byte FREE = 2;
	private final byte[] alphaStatus;
	private final double[] alpha;
	private final OcsvmKernel kernel;
	private final double[] diagonal;
	private final double epsilon;
	private final double[] p;
	private final int[] activeSet;
	private final double[] gradientBar;
	private final int numSample;
	private boolean unShrink;
	private final int shrinking;
	private static final double INF = Double.POSITIVE_INFINITY;

	public SmoSolver(int numSample, OcsvmKernel kernel, double[] alpha, double epsilon, int shrinking) {
		this.numSample = numSample;
		this.kernel = kernel;
		diagonal = kernel.getDial().getData();

		p = new double[numSample];
		y = new byte[numSample];
		for (int i = 0; i < numSample; i++) {
			p[i] = 0;
			y[i] = 1;
		}

		this.alpha = alpha;
		this.epsilon = epsilon;
		this.unShrink = false;
		this.shrinking = shrinking;

		/* Initializes alpha status */
		alphaStatus = new byte[numSample];
		for (int i = 0; i < numSample; i++) {
			updateAlphaStatus(i);
		}

		/* Initializes activeSet */
		activeSet = new int[numSample];
		for (int i = 0; i < numSample; i++) {
			activeSet[i] = i;
		}
		activeSize = numSample;

		/* initialize gradient */
		gradient = new double[numSample];
		gradientBar = new double[numSample];
		for (int i = 0; i < numSample; i++) {
			gradient[i] = p[i];
			gradientBar[i] = 0;
		}
		for (int i = 0; i < numSample; i++) {
			if (alphaStatus[i] != LOWER_BOUND) {
				double[] qI = kernel.getData(i, numSample);
				double alphaI = this.alpha[i];
				for (int j = 0; j < numSample; j++) {
					gradient[j] += alphaI * qI[j];
				}
				if (alphaStatus[i] == UPPER_BOUND) {
					for (int j = 0; j < numSample; j++) {
						gradientBar[j] += qI[j];
					}
				}
			}
		}
	}

	void updateAlphaStatus(int i) {
		if (alpha[i] >= 1.0) {
			alphaStatus[i] = UPPER_BOUND;
		} else if (alpha[i] <= 0) {
			alphaStatus[i] = LOWER_BOUND;
		} else {
			alphaStatus[i] = FREE;
		}
	}

	boolean isFree(int i) { return alphaStatus[i] == FREE; }

	void swapIndex(int i, int j) {
		kernel.swapIndex(i, j);
		byte tmpByte = this.y[i];
		this.y[i] = this.y[j];
		this.y[j] = tmpByte;
		double tmp = this.gradient[i];
		this.gradient[i] = this.gradient[j];
		this.gradient[j] = tmp;
		tmpByte = this.alphaStatus[i];
		this.alphaStatus[i] = this.alphaStatus[j];
		this.alphaStatus[j] = tmpByte;
		tmp = this.alpha[i];
		this.alpha[i] = this.alpha[j];
		this.alpha[j] = tmp;
		tmp = this.p[i];
		this.p[i] = this.p[j];
		this.p[j] = tmp;
		int tmpInt = this.activeSet[i];
		this.activeSet[i] = this.activeSet[j];
		this.activeSet[j] = tmpInt;
		tmp = this.gradientBar[i];
		this.gradientBar[i] = this.gradientBar[j];
		this.gradientBar[j] = tmp;
	}

	void reconstructGradient() {
		if (activeSize == numSample) {
			return;
		}

		int nrFree = 0;
		for (int j = activeSize; j < numSample; j++) {
			gradient[j] = gradientBar[j] + p[j];
		}

		for (int j = 0; j < activeSize; j++) {
			if (isFree(j)) {
				nrFree++;
			}
		}

		if (nrFree * numSample > 2 * activeSize * (numSample - activeSize)) {
			for (int i = activeSize; i < numSample; i++) {
				double[] qI = kernel.getData(i, activeSize);
				for (int j = 0; j < activeSize; j++) {
					if (isFree(j)) {
						gradient[i] += alpha[j] * qI[j];
					}
				}
			}
		} else {
			for (int i = 0; i < activeSize; i++) {
				if (isFree(i)) {
					double[] qi = kernel.getData(i, numSample);
					double alphaI = alpha[i];
					for (int j = activeSize; j < numSample; j++) {
						gradient[j] += alphaI * qi[j];
					}
				}
			}
		}
	}

	public Tuple2 <Double, Double> solve() {
		int iter = 0;
		int maxIter = Math.max(10000000, numSample > Integer.MAX_VALUE / 100 ? Integer.MAX_VALUE : 100 * numSample);
		int counter = Math.min(numSample, 1000) + 1;

		int[] workingSet = new int[2];
		while (iter < maxIter) {
			if (--counter == 0) {
				counter = Math.min(numSample, 1000);
				if (shrinking != 0) {
					doShrinking();
				}
			}
			if (selectWorkingSet(workingSet) != 0) {
				reconstructGradient();
				activeSize = numSample;
				if (selectWorkingSet(workingSet) != 0) {
					break;
				} else {
					counter = 1;
				}
			}

			double[] qI = kernel.getData(workingSet[0], activeSize);
			double[] qJ = kernel.getData(workingSet[1], activeSize);

			double cI = 1.0;
			double cJ = 1.0;

			double oldAlphaI = alpha[workingSet[0]];
			double oldAlphaJ = alpha[workingSet[1]];

			if (y[workingSet[0]] != y[workingSet[1]]) {
				double quadCoef = diagonal[workingSet[0]] + diagonal[workingSet[1]] + 2 * qI[workingSet[1]];
				if (quadCoef <= 0) {
					quadCoef = 1e-12;
				}
				double delta = (-gradient[workingSet[0]] - gradient[workingSet[1]]) / quadCoef;
				double diff = alpha[workingSet[0]] - alpha[workingSet[1]];
				alpha[workingSet[0]] += delta;
				alpha[workingSet[1]] += delta;

				if (diff > 0) {
					if (alpha[workingSet[1]] < 0) {
						alpha[workingSet[1]] = 0;
						alpha[workingSet[0]] = diff;
					}
				} else {
					if (alpha[workingSet[0]] < 0) {
						alpha[workingSet[0]] = 0;
						alpha[workingSet[1]] = -diff;
					}
				}
				if (diff > cI - cJ) {
					if (alpha[workingSet[0]] > cI) {
						alpha[workingSet[0]] = cI;
						alpha[workingSet[1]] = cI - diff;
					}
				} else {
					if (alpha[workingSet[1]] > cJ) {
						alpha[workingSet[1]] = cJ;
						alpha[workingSet[0]] = cJ + diff;
					}
				}
			} else {
				double quadCoef = diagonal[workingSet[0]] + diagonal[workingSet[1]] - 2 * qI[workingSet[1]];
				if (quadCoef <= 0) {
					quadCoef = 1e-12;
				}
				double delta = (gradient[workingSet[0]] - gradient[workingSet[1]]) / quadCoef;
				double sum = alpha[workingSet[0]] + alpha[workingSet[1]];
				alpha[workingSet[0]] -= delta;
				alpha[workingSet[1]] += delta;

				if (sum > cI) {
					if (alpha[workingSet[0]] > cI) {
						alpha[workingSet[0]] = cI;
						alpha[workingSet[1]] = sum - cI;
					}
				} else {
					if (alpha[workingSet[1]] < 0) {
						alpha[workingSet[1]] = 0;
						alpha[workingSet[0]] = sum;
					}
				}
				if (sum > cJ) {
					if (alpha[workingSet[1]] > cJ) {
						alpha[workingSet[1]] = cJ;
						alpha[workingSet[0]] = sum - cJ;
					}
				} else {
					if (alpha[workingSet[0]] < 0) {
						alpha[workingSet[0]] = 0;
						alpha[workingSet[1]] = sum;
					}
				}
			}

			/* update gradient */
			double deltaAlphaI = alpha[workingSet[0]] - oldAlphaI;
			double deltaAlphaJ = alpha[workingSet[1]] - oldAlphaJ;

			for (int k = 0; k < activeSize; k++) {
				gradient[k] += qI[k] * deltaAlphaI + qJ[k] * deltaAlphaJ;
			}

			/* update alpha status and gradient bar */
			boolean isUpperI = (alphaStatus[workingSet[0]] == UPPER_BOUND);
			boolean isUpperJ = (alphaStatus[workingSet[1]] == UPPER_BOUND);
			updateAlphaStatus(workingSet[0]);
			updateAlphaStatus(workingSet[1]);
			if (isUpperI != (alphaStatus[workingSet[0]] == UPPER_BOUND)) {
				qI = kernel.getData(workingSet[0], numSample);
				if (isUpperI) {
					for (int k = 0; k < numSample; k++) {
						gradientBar[k] -= cI * qI[k];
					}
				} else {
					for (int k = 0; k < numSample; k++) {
						gradientBar[k] += cI * qI[k];
					}
				}
			}

			if (isUpperJ != (alphaStatus[workingSet[1]] == UPPER_BOUND)) {
				qJ = kernel.getData(workingSet[1], numSample);
				if (isUpperJ) {
					for (int k = 0; k < numSample; k++) {
						gradientBar[k] -= cJ * qJ[k];
					}
				} else {
					for (int k = 0; k < numSample; k++) {
						gradientBar[k] += cJ * qJ[k];
					}
				}
			}
			iter ++;
		}
		if (iter >= maxIter) {
			if (activeSize < numSample) {
				reconstructGradient();
				activeSize = numSample;
			}
			System.err.print("WARNING: not converged at maxStep.");
		}
		/* calculate rho */
		double rho = calculateRho();
		double obj;

		/* calculate objective value */
		double objVal = 0;
		for (int i = 0; i < numSample; i++) {
			objVal += alpha[i] * (gradient[i] + p[i]);
		}
		obj = objVal / 2;
		if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
			System.out.println("SMO finished at step = " + iter);
		}
		return Tuple2.of(rho, obj);
	}

	int selectWorkingSet(int[] workingSet) {
		double gmax = -INF;
		double gmax2 = -INF;
		int gmaxIdx = -1;
		int gminIdx = -1;
		double objDiffMin = INF;
		for (int t = 0; t < activeSize; t++) {
			if (y[t] == 1) {
				if (alphaStatus[t] != UPPER_BOUND) {
					if (-gradient[t] >= gmax) {
						gmax = -gradient[t];
						gmaxIdx = t;
					}
				}
			} else {
				if (alphaStatus[t] != LOWER_BOUND) {
					if (gradient[t] >= gmax) {
						gmax = gradient[t];
						gmaxIdx = t;
					}
				}
			}
		}
		int idx = gmaxIdx;
		double[] qI = null;
		if (idx != -1) {
			qI = kernel.getData(idx, activeSize);
		}
		for (int j = 0; j < activeSize; j++) {
			if (y[j] == 1) {
				if (alphaStatus[j] != LOWER_BOUND) {
					double gradDiff = gmax + gradient[j];
					if (gradient[j] >= gmax2) {
						gmax2 = gradient[j];
					}
					if (gradDiff > 0) {
						double objDiff;
						double quadCoef = diagonal[idx] + diagonal[j] - 2.0 * y[idx] * qI[j];
						if (quadCoef > 0) {
							objDiff = -(gradDiff * gradDiff) / quadCoef;
						} else {
							objDiff = -(gradDiff * gradDiff) / 1e-12;
						}
						if (objDiff <= objDiffMin) {
							gminIdx = j;
							objDiffMin = objDiff;
						}
					}
				}
			} else {
				if (alphaStatus[j] != UPPER_BOUND) {
					double gradDiff = gmax - gradient[j];
					if (-gradient[j] >= gmax2) {
						gmax2 = -gradient[j];
					}
					if (gradDiff > 0) {
						double objDiff;
						double quadCoef = diagonal[idx] + diagonal[j] + 2.0 * y[idx] * qI[j];
						if (quadCoef > 0) {
							objDiff = -(gradDiff * gradDiff) / quadCoef;
						} else {
							objDiff = -(gradDiff * gradDiff) / 1e-12;
						}
						if (objDiff <= objDiffMin) {
							gminIdx = j;
							objDiffMin = objDiff;
						}
					}
				}
			}
		}
		if (gmax + gmax2 < epsilon) {
			return 1;
		}
		workingSet[0] = gmaxIdx;
		workingSet[1] = gminIdx;
		return 0;
	}

	private boolean beShrunk(int i, double gmax1, double gmax2) {
		if (alphaStatus[i] == UPPER_BOUND) {
			if (y[i] == 1) {
				return (-gradient[i] > gmax1);
			} else {
				return (-gradient[i] > gmax2);
			}
		} else if (alphaStatus[i] == LOWER_BOUND) {
			if (y[i] == 1) {
				return (gradient[i] > gmax2);
			} else {
				return (gradient[i] > gmax1);
			}
		} else {
			return (false);
		}
	}

	void doShrinking() {
		double gmax1 = -INF;
		double gmax2 = -INF;
		/* find mvp first */
		for (int i = 0; i < activeSize; i++) {
			if (y[i] == 1) {
				if (alphaStatus[i] != UPPER_BOUND) {
					if (-gradient[i] >= gmax1) {
						gmax1 = -gradient[i];
					}
				}
				if (alphaStatus[i] != LOWER_BOUND) {
					if (gradient[i] >= gmax2) {
						gmax2 = gradient[i];
					}
				}
			} else {
				if (alphaStatus[i] != UPPER_BOUND) {
					if (-gradient[i] >= gmax2) {
						gmax2 = -gradient[i];
					}
				}
				if (alphaStatus[i] != LOWER_BOUND) {
					if (gradient[i] >= gmax1) {
						gmax1 = gradient[i];
					}
				}
			}
		}
		if (!unShrink && gmax1 + gmax2 <= epsilon * 10) {
			unShrink = true;
			reconstructGradient();
			activeSize = numSample;
		}

		for (int i = 0; i < activeSize; i++) {
			if (beShrunk(i, gmax1, gmax2)) {
				activeSize--;
				while (activeSize > i) {
					if (!beShrunk(activeSize, gmax1, gmax2)) {
						swapIndex(i, activeSize);
						break;
					}
					activeSize--;
				}
			}
		}
	}

	double calculateRho() {
		int nrFree = 0;
		double upperBound = INF, lowerBound = -INF, sumFree = 0;
		for (int i = 0; i < activeSize; i++) {
			double yDotGradient = y[i] * gradient[i];
			if (alphaStatus[i] == LOWER_BOUND) {
				if (y[i] > 0) {
					upperBound = Math.min(upperBound, yDotGradient);
				} else {
					lowerBound = Math.max(lowerBound, yDotGradient);
				}
			} else if (alphaStatus[i] == UPPER_BOUND) {
				if (y[i] < 0) {
					upperBound = Math.min(upperBound, yDotGradient);
				} else {
					lowerBound = Math.max(lowerBound, yDotGradient);
				}
			} else {
				++nrFree;
				sumFree += yDotGradient;
			}
		}
		if (nrFree > 0) {
			return sumFree / nrFree;
		} else {
			return (upperBound + lowerBound) / 2;
		}
	}
}

