package com.alibaba.alink.common.probabilistic;

import com.alibaba.alink.common.utils.XMath;

/**
 * Probability Density Function
 */
public class PDF {

	public static double poisson(int n, double lambda) {
		if ((lambda <= 0) || (n < 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (n == 0) {
			return Math.exp(-lambda);
		} else if (n == 1) {
			return lambda * Math.exp(-lambda);
		} else {
			double r = ((double) n) * Math.log(lambda);
			for (int k = 2; k <= n; k++) {
				r -= Math.log((double) k);
			}
			return Math.exp(r - lambda);
		}
	}

	public static double uniform(double x, double lowerBound, double upperBound) {
		if (lowerBound >= upperBound) {
			throw new RuntimeException("Wrong input parameters: the lower bound should be less than the upper bound!");
		}
		if (x < lowerBound || x > upperBound) {
			return 0.0;
		} else {
			return 1 / (upperBound - lowerBound);
		}
	}

	public static double exponential(double x, double lambda) {
		if (lambda <= 0) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (x < 0) {
			return 0;
		} else {
			return lambda * Math.exp(-lambda * x);
		}
	}

	public static double stdNormal(double x) {
		return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
	}

	public static double normal(double x, double mu, double sigma2) throws Exception {
		if (sigma2 <= 0) {
			throw new Exception("Input parameter out of range!");
		}
		double sigma = Math.sqrt(sigma2);
		double t = (x - mu) / sigma;
		return stdNormal(t) / sigma;
	}

	public static double chi2(double x, double df) {
		if (x <= 0) {
			return 0.0;
		} else {
			double n2 = (double) df / 2;
			return Math.exp(-x / 2 + (n2 - 1) * Math.log(x) - n2 * Math.log(2.0) - XMath.lnGamma(n2));
		}
	}

	public static double studentT(double x, double df) {
		return Math.exp(-(df + 1.0) * Math.log(1 + x * x / df) / 2.0 + XMath.lnGamma((df + 1) / 2.0) - XMath
			.lnGamma(df / 2.0) - Math.log(df) / 2.0 - Math.log(Math.PI) / 2.0);
	}

	public static double F(double x, double df1, double df2) {
		if (x <= 0) {
			return 0.0;
		} else {
			return Math.exp(df1 * (Math.log(df1) - Math.log(df2)) / 2.0 + (df1 / 2.0 - 1) * Math.log(x)
				- (df1 + df2) * Math.log(1.0 + x * df1 / df2) / 2.0 - XMath.lnGamma(df2 / 2.0) - XMath
				.lnGamma(df1 / 2.0) + XMath.lnGamma((df1 + df2) / 2.0));
		}
	}

	public static double gamma(double x, double alpha, double lambda) {
		if ((alpha <= 0) || (lambda <= 0) || (x <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		return Math.exp(alpha * Math.log(lambda) + (alpha - 1) * Math.log(x) - lambda * x - XMath.lnGamma(alpha));
	}

	public static double beta(double x, double a, double b) {
		if ((x < 0) || (x > 1) || (a <= 0) || (b <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		return Math.exp(
			XMath.lnGamma(a + b) - XMath.lnGamma(a) - XMath.lnGamma(b) + (a - 1) * Math.log(x) + (b - 1) * Math
				.log(1 - x));
	}

	public static double fisherZ(double x, double p, double q) {
		if ((p <= 0) || (q <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (x <= 0) {
			return 0;
		} else {
			double r = (p - 1) * Math.log(x) - (p + q) * Math.log(1 + x);
			return Math.exp(r - XMath.lnGamma(p) - XMath.lnGamma(q) + XMath.lnGamma(p + q));
		}
	}
}
