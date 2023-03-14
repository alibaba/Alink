package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.distance.EuclideanDistance;
import com.alibaba.alink.operator.common.distance.FastDistance;
import com.alibaba.alink.operator.common.distance.FastDistanceVectorData;
import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.special.Gamma;

import java.util.ArrayList;
import java.util.List;

public class TreeParzenEstimator {
	private final List <DenseVector> X;
	private final DenseVector y;
	private final FastDistance fastDistance = new EuclideanDistance();

	private FastDistanceVectorData[] fastDistanceVectors_l;
	private FastDistanceVectorData[] fastDistanceVectors_g;

	private List <DenseVector> records_l;
	private List <DenseVector> records_g;

	private static final double EPS = 1e-12;
	private static final double BANDWIDTH = 1.0;

	public TreeParzenEstimator(List <DenseVector> X, List <Double> y) {
		if (null == X || null == y) {
			throw new IllegalArgumentException("Input CAN NOT be null!");
		}
		if (X.size() != y.size()) {
			throw new IllegalArgumentException("X and y MUST have the same size!");
		}
		this.X = X;
		int n = this.X.size();
		this.y = new DenseVector(n);

		double y_mean = 0;
		for (int i = 0; i < n; i++) {
			this.y.set(i, y.get(i));
			y_mean += y.get(i) / n;
		}

		int n1 = 0;
		for (int i = 0; i < n; i++) {
			if (y.get(i) >= y_mean) {
				n1++;
			}
		}

		// prepare data
		fastDistanceVectors_l = new FastDistanceVectorData[n1];
		fastDistanceVectors_g = new FastDistanceVectorData[n - n1];

		records_l = new ArrayList <>(n1);
		records_g = new ArrayList <>(n - n1);

		int idx_l = 0;
		int idx_g = 0;
		for (int k = 0; k < n; k += 1) {
			if (y.get(k) >= y_mean) {
				fastDistanceVectors_l[idx_l++] = fastDistance.prepareVectorData(Row.of(X.get(k), k), 0, 1);
				records_l.add(X.get(k));
			} else {
				fastDistanceVectors_g[idx_g++] = fastDistance.prepareVectorData(Row.of(X.get(k), k), 0, 1);
				records_g.add(X.get(k));
			}
		}

	}

	public double KDEPdf(DenseVector sample, FastDistanceVectorData[] X) {
		if (X.length == 0) {return 0;}
		int vectorSize = sample.size();

		int n = X.length;
		//calculate distances
		double[] kNeighborDists = new double[n];
		for (int k = 0; k < n; k++) {
			kNeighborDists[k] = fastDistance.calc(X[k].getVector(), sample);
		}

		// Calculate lde (local dense estimate) 
		double kde = 0;
		double score;
		for (int i = 0; i < n; i += 1) {
			double rd = kNeighborDists[i];
			double logKernel = this.logKernelFunction(BANDWIDTH, rd);
			kde = i == 0 ? logKernel : logAddExp(kde, logKernel);
		}

		// Calculate log-likelihood
		score = logKernelNorm(BANDWIDTH, vectorSize) + kde - Math.log(n);

		return score;
	}

	public double calc(DenseVector sample) {
		double y_l = KDEPdf(sample, fastDistanceVectors_l);
		double y_g = KDEPdf(sample, fastDistanceVectors_g);
		return y_l - y_g;
	}

	public int calc(DenseVector sample, int dim, Double mu, Double sigma, Double low, Double high, Integer q, int linearForgetting) {
		List <DenseVector> normal_l = parzenNormal(records_l, dim, mu, sigma, linearForgetting);
		List <DenseVector> normal_g = parzenNormal(records_g, dim, mu, sigma, linearForgetting);
		DenseVector yl = GMMPdf(sample, normal_l.get(0), normal_l.get(1), normal_l.get(2), low, high, q);
		DenseVector yg = GMMPdf(sample, normal_g.get(0), normal_g.get(1), normal_g.get(2), low, high, q);
		double[] prob = yg.minus(yl).getData();
		double bestScore = prob[0];
		int bestSampleId = 0;
		for (int i = 1; i < prob.length; i++) {
			if (prob[i] > bestScore) {
				bestScore = prob[i];
				bestSampleId = i;
			}
		}
		return bestSampleId;
	}

	private static double LOG_2PI = Math.log(2 * Math.PI);

	private static double LOG_PI = Math.log(Math.PI);

	private static double logVn(int n) {
		return 0.5 * n * LOG_PI - Gamma.logGamma(0.5 * n + 1);
	}

	private static double logSn(int n) {
		return LOG_2PI + logVn(n - 1);
	}

	private static double logAddExp(double x1, double x2) {
		return Math.log(Math.exp(x1) + Math.exp(x2));
	}

	private double logKernelNorm(double h, int d) {
		double factor = 0;
		factor = 0.5 * d * LOG_2PI;
		return -factor - d * Math.log(h);
	}

	private double logKernelFunction(double h, double dist) {
		return -0.5 * (dist * dist) / (h * h);
	}

	private  DenseVector GMMPdf(DenseVector samples, DenseVector weights, DenseVector mus, DenseVector sigmas,
									  Double low, Double high, Integer q) {
		double pAccept = 0.0;
		int n_samples = samples.size();
		int n_components = sigmas.size();

		if (low == null || high == null) {
			pAccept = 1.0;
		} else {
			pAccept = weights.dot(
				normalCdf(high, mus, sigmas)
					.minus(
						normalCdf(low, mus, sigmas)
					)
			);
		}

		if (q == null) {
			double[][] mahal = new double[n_samples][n_components];
			double[][] z = new double[n_samples][n_components];
			double[] coef = new double[n_components];

			for (int i = 0; i < n_components; i++) {
				coef[i] = Math.sqrt(2.0 * Math.PI * Math.pow(sigmas.get(i), 2.0));
				coef[i] = weights.get(i) / coef[i] / pAccept;
			}

			for (int i = 0; i < n_samples; i++) {
				for (int j = 0; j < n_components; j++) {
					mahal[i][j] = samples.get(i) - mus.get(j);
					mahal[i][j] /= Math.max(sigmas.get(j), EPS);
					mahal[i][j] = Math.pow(mahal[i][j], 2.0);
					z[i][j] = Math.log(coef[j]) - mahal[i][j] * 0.5;
				}
			}

			return logSum(z);
		} else {
			DenseVector prob = new DenseVector(samples.size());
			DenseVector uBound = samples.clone();
			DenseVector lBound = samples.clone();
			DenseVector tol = DenseVector.ones(n_samples).scale(q / 2.0);
			uBound.plusEqual(tol);
			lBound.minusEqual(tol);

			for (int i = 0; i < n_samples; i++) {
				if (high != null) {
					uBound.set(i,
						Math.min(uBound.get(i), high)
					);
				}

				if (low != null) {
					lBound.set(i,
						Math.max(lBound.get(i), low)
					);
				}
			}

			for (int i = 0; i < n_components; i++) {
				double w = weights.get(i);
				double mu = mus.get(i);
				double sigma = sigmas.get(i);
				DenseVector incAmt = normalCdf(uBound, mu, sigma);
				incAmt.minusEqual(normalCdf(lBound, mu, sigma));
				incAmt.scaleEqual(w);
				prob.plusEqual(incAmt);
			}

			DenseVector result = new DenseVector(n_samples);
			pAccept = Math.log(pAccept);
			for (int i = 0; i < n_samples; i++) {
				result.set(i, Math.log(prob.get(i)) - pAccept);
			}

			return result;
		}
	}

	private List <DenseVector> parzenNormal(List <DenseVector> records, int dim, double priorMu, double priorSigma, int linearForgetting) {
		int priorPos = 0;
		double[] srtdMus, sigma, srtdWeight;
		double[] mus = new double[records.size()];
		srtdWeight = new double[records.size() + 1];
		for (int i = 0; i < records.size(); i++) {
			mus[i] = records.get(i).get(dim);
		}

		if (mus.length == 0) {
			srtdMus = new double[] {priorMu};
			sigma = new double[] {priorSigma};
		} else if (mus.length == 1) {
			if (priorMu < mus[0]) {
				srtdMus = new double[] {priorMu, mus[0]};
				sigma = new double[] {priorSigma, priorSigma * 0.5};
			} else {
				priorPos = 1;
				srtdMus = new double[] {mus[0], priorMu};
				sigma = new double[] {priorSigma * 0.5, priorSigma};
			}
		} else {
			double minimalValue;
			int[] order = new int[mus.length + 1];
			srtdMus = new double[mus.length + 1];
			sigma = new double[mus.length + 1];

			System.arraycopy(mus, 0, srtdMus, 0, mus.length);
			srtdMus[mus.length] = priorMu;
			for (int i = 0; i < order.length - 1; i++) {
				minimalValue = srtdMus[i];
				int k = i;
				for (int j = i + 1; j < order.length; j++) {
					if (srtdMus[j] < minimalValue) {
						minimalValue = srtdMus[j];
						k = j;
					}
				}
				order[k] = i;
			}

			for (int i = 1; i < order.length - 1; i++) {
				sigma[i] = Math.max(srtdMus[i] - srtdMus[i - 1], srtdMus[i + 1] - srtdMus[i]);
			}
			sigma[0] = srtdMus[1] - srtdMus[0];
			sigma[order.length - 1] = srtdMus[order.length - 1] - srtdMus[order.length - 2];

			if (linearForgetting < mus.length) {
				double[] unsort_weight = linearWeights(mus.length, linearForgetting);
				for (int i = 0; i < unsort_weight.length; i++) {
					srtdWeight[order[i]] = unsort_weight[i];
				}
				srtdWeight[order[mus.length]] = unsort_weight[mus.length-1];
			}
		}

		if (linearForgetting >= mus.length) {
			for (int i = 0; i < srtdWeight.length; i++) {
				srtdWeight[i] = 1.0 / srtdWeight.length;
			}
		}

		double minSigma = priorSigma / Math.min(100, 1 + sigma.length);
		for (int i = 0; i < sigma.length; i++) {
			sigma[i] = sigma[i] > priorSigma ? priorSigma : sigma[i];
			sigma[i] = sigma[i] < minSigma ? minSigma : sigma[i];
		}

		ArrayList <DenseVector> result = new ArrayList <>(3);
		result.add(new DenseVector(srtdWeight));
		result.add(new DenseVector(srtdMus));
		result.add(new DenseVector(sigma));

		return result;
	}

	private double[] linearWeights(int n, int lf) {
		double start = 1.0 / (double) n;
		double[] weight = new double[n];
		int stepNum = n - lf - 1;
		if (stepNum == 0) {
			weight[0] = start;
			for (int i = 1; i < n; i++) {
				weight[i] = 1.0;
			}
			return weight;
		}

		double step = (1.0 - start) / stepNum;
		weight[0] = start;
		for (int i = 1; i < n - lf; i++) {
			weight[i] = weight[i - 1] + step;
		}
		for (int i = n - lf; i < n; i++) {
			weight[i] = 1.0;
		}
		return weight;
	}

	private static DenseVector normalCdf(double x, DenseVector mu, DenseVector sigma) {
		DenseVector top = DenseVector.ones(mu.size()).scale(x);
		top.minusEqual(mu);
		DenseVector bottom = sigma.scale(Math.sqrt(2.0));
		DenseVector z = new DenseVector(mu.size());
		for (int i = 0; i < z.size(); i++) {
			double z_ = EPS > bottom.get(i) ? top.get(i) / EPS : top.get(i) / bottom.get(i);
			z.set(i, 0.5 * (1 + Erf.erf(z_)));
		}
		return z;
	}

	private static DenseVector normalCdf(DenseVector x, double mu, double sigma) {
		DenseVector top = x.minus(DenseVector.ones(x.size()).scale(mu));
		double bottom = Math.max(EPS, sigma * Math.sqrt(2.0));
		DenseVector z = new DenseVector(x.size());
		for (int i = 0; i < z.size(); i++) {
			z.set(i, 0.5 * (1 + Erf.erf(top.get(i) / bottom)));
		}
		return z;
	}

	private static DenseVector logSum(double[][] x) {
		double[] m = new double[x.length];
		double[] logSum = new double[x.length];
		for (int i = 0; i < m.length; i++) {
			double maxValue = x[i][0];
			for (int j = 1; j < x[i].length; j++) {
				maxValue = maxValue < x[i][j] ? x[i][j] : maxValue;
			}
			m[i] = maxValue;
			for (int j = 0; j < x[i].length; j++) {
				logSum[i] += Math.exp(x[i][j] - maxValue);
			}
		}

		for (int i = 0; i < m.length; i++) {
			m[i] += Math.log(logSum[i]);
		}

		return new DenseVector(m);
	}
	
}
