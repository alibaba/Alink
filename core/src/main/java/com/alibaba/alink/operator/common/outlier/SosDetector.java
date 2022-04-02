package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.linalg.MatVecOp;
import com.alibaba.alink.common.linalg.Vector;

import java.util.Map;

/**
 * An implementation of the Stochastic Outlier Selection algorithm by Jeroen Jansen
 * <p>
 * For more information about SOS, see https://github.com/jeroenjanssens/sos
 * J.H.M. Janssens, F. Huszar, E.O. Postma, and H.J. van den Herik. Stochastic
 * Outlier Selection. Technical Report TiCC TR 2012-001, Tilburg University,
 * Tilburg, the Netherlands, 2012.
 */
public class SosDetector extends OutlierDetector {

	private static final int MAX_ITER = 100;
	private static final double TOL = 1.0e-2;
	private final double perplexity;

	public SosDetector(TableSchema dataSchema,
					   Params params) {
		super(dataSchema, params);
		perplexity = params.get(SosDetectorParams.PERPLEXITY);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		Vector[] vectors = OutlierUtil.getVectors(series, params);
		final int n = vectors.length;
		double[][] matrixD = new double[n][n];
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				matrixD[i][j] = MatVecOp.minus(vectors[i], vectors[j]).normL2Square();
			}
		}

		double[] buffer = new double[n];

		double[][] matrixB = new double[n][n];
		for (int i = 0; i < n; i++) {
			double[] vectorDi = matrixD[i];

			// beta: 1 / (2 * sigma^2)
			// compute beta by solving a nonlinear equation
			double beta = solveForBeta(i, vectorDi, 1.0, perplexity, MAX_ITER, TOL, buffer);

			// (1) the affinity that data point xi has with data point xj decays Gaussian-like with
			// respect to the dissimilarity dij
			// (2) a data point has no affinity with itself, i.e., aii = 0

			// compute the affinity
			double s = 0.;
			for (int j = 0; j < n; j++) {
				if (j != i) {
					double v = Math.exp(-vectorDi[j] * beta);
					s += v;
					matrixB[i][j] = v;
				}
			}

			// compute the binding probability
			for (int j = 0; j < n; j++) {
				matrixB[i][j] /= s;
			}
		}

		double[] outlierProbability = new double[n];
		for (int i = 0; i < n; i++) {
			double s = 1.0;
			for (int j = 0; j < n; j++) {
				s *= (1.0 - matrixB[j][i]);
			}
			outlierProbability[i] = s;
		}

		double K = 0.5;

		//if (detectLast) {
		//	return new Tuple2[] {Tuple2.of(
		//		Math.abs(outlierProbability[outlierProbability.length - 1]) > K ? true : false,
		//		"Score: " + outlierProbability[outlierProbability.length - 1])};
		//} else {
		//	Tuple2 <Boolean, String>[] results = new Tuple2[outlierProbability.length];
		//	for (int i = 0; i < outlierProbability.length; i++) {
		//		results[i] = Tuple2.of(Math.abs(outlierProbability[i]) > K ? true : false,
		//			"Score: " + outlierProbability[i]);
		//	}
		//	return results;
		//}
		int iStart = detectLast ? outlierProbability.length - 1 : 0;
		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[outlierProbability.length - iStart];
		for (int i = iStart; i < outlierProbability.length; i++) {
			if (isPredDetail) {
				results[i] = Tuple3.of(Math.abs(outlierProbability[i]) > K, outlierProbability[i], null);
			} else {
				results[i] = Tuple3.of(Math.abs(outlierProbability[i]) > K, null, null);
			}
		}
		return results;

	}

	private static double solveForBeta(int idx, double[] d, double beta0, double h, int maxIter, double tol,
									   double[] buffer) {
		int iter = 0;
		double beta = beta0;
		double logh = Math.log(h);
		double hCurr = computeLogH(idx, d, beta, buffer);
		double err = hCurr - logh;
		double betaMin = 0.;
		double betaMax = Double.MAX_VALUE;

		while (iter < maxIter && (Double.isNaN(err) || Math.abs(err) > tol)) {
			if (Double.isNaN(err)) {
				beta = beta / 10.0;
			} else {
				if (err > 0.) { // should increase beta
					if (betaMax == Double.MAX_VALUE) {
						betaMin = beta;
						beta *= 2;
					} else {
						betaMin = beta;
						beta = 0.5 * (betaMin + betaMax);
					}
				} else { // should decrease beta
					betaMax = beta;
					beta = 0.5 * (betaMin + beta);
				}
			}
			iter++;

			hCurr = computeLogH(idx, d, beta, buffer);
			err = hCurr - logh;
		}

		if (Double.isNaN(err)) {
			beta = beta / 2.0;
		}

		return beta;
	}

	private static double computeLogH(int idx, double[] d, double beta, double[] buffer) {
		double[] affinity = buffer;
		double sumA = 0.;
		for (int i = 0; i < affinity.length; i++) {
			if (i == idx) {
				affinity[i] = 0.;
			} else {
				affinity[i] = Math.exp(-beta * d[i]);
				sumA += affinity[i];
			}
		}

		double logh = 0.;
		for (int i = 0; i < affinity.length; i++) {
			double v = affinity[i] * d[i];
			logh += v;
		}
		logh *= (beta / sumA);
		logh += Math.log(sumA);

		return logh;
	}
}
