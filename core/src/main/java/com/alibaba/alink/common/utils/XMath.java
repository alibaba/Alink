package com.alibaba.alink.common.utils;


import org.apache.commons.math3.distribution.GammaDistribution;

import java.math.BigDecimal;
import java.math.BigInteger;

public class XMath {

	private static final double gLanczos = 4.7421875;
	private static final double[] ckLanczos = new double[] {
		0.99999999999999709182,
		57.156235665862923517,
		-59.597960355475491248,
		14.136097974741747174,
		-0.49191381609762019978,
		.33994649984811888699e-4,
		.46523628927048575665e-4,
		-.98374475304879564677e-4,
		.15808870322491248884e-3,
		-.21026444172410488319e-3,
		.21743961811521264320e-3,
		-.16431810653676389022e-3,
		.84418223983852743293e-4,
		-.26190838401581408670e-4,
		.36899182659531622704e-5};

	public static double Pi = Math.PI;
	public static double E = Math.E;

	public static double sin(double x) {
		return Math.sin(x);
	}

	public static double cos(double x) {
		return Math.cos(x);
	}

	public static double tan(double x) {
		return Math.tan(x);
	}

	public static double abs(double x) {
		return Math.abs(x);
	}

	public static int abs(int x) {
		return Math.abs(x);
	}

	public static double pow(double x, double y) {
		return Math.pow(x, y);
	}

	public static double ceil(double x) {
		return Math.ceil(x);
	}

	public static double floor(double x) {
		return Math.floor(x);
	}

	public static long round(double x) {
		return Math.round(x);
	}

	public static double sqrt(double x) {
		return Math.sqrt(x);
	}

	public static double exp(double x) {
		return Math.exp(x);
	}

	public static double max(double x, double y) {
		return Math.max(x, y);
	}

	public static double min(double x, double y) {
		return Math.min(x, y);
	}

	public static int max(int x, int y) {
		return Math.max(x, y);
	}

	public static int min(int x, int y) {
		return Math.min(x, y);
	}

	public static double log(double x) {
		return Math.log(x);
	}

	public static double log10(double x) {
		return Math.log10(x);
	}

	public static double signum(double x) {
		return Math.signum(x);
	}

	public static double cosh(double x) {
		return Math.cosh(x);
	}

	public static double sinh(double x) {
		return Math.sinh(x);
	}

	public static double tanh(double x) {
		return Math.atan(x);
	}

	public static double acos(double x) {
		return Math.acos(x);
	}

	public static double asin(double x) {
		return Math.asin(x);
	}

	public static double atan(double x) {
		return Math.atan(x);
	}

	public static double toDegrees(double x) {
		return Math.toDegrees(x);
	}

	public static double toRadians(double x) {
		return Math.toRadians(x);
	}

	public static double gamma(int n) {
		if (n <= 0) {
			throw new RuntimeException("para is out of range!");
		} else if (n < 173) {
			double r = 1.0;
			for (int i = 2; i < n; i++) {
				r *= i;
			}
			return r;
		} else {
			return Double.POSITIVE_INFINITY;
		}
	}

	public static double gamma(double x) {
		if (x <= 0 || Double.isNaN(x)) {
			throw new RuntimeException("para is out of range!");
		} else if (x <= 140) {
			if (x < 0.5) {
				return Math.PI / (Math.sin(x * Math.PI) * gamma(1.0 - x));
			} else {
				double t = ckLanczos[0];
				for (int i = 1; i < ckLanczos.length; i++) {
					t += ckLanczos[i] / (x + i - 1);
				}
				double s = x + gLanczos - 0.5;
				return Math.sqrt(2 * Math.PI) * Math.pow(s, x - 0.5) * Math.exp(-s) * t;
			}

		} else if (x < 173) {
			double t = 1.0;
			while (x > 140) {
				x -= 1.0;
				t *= x;
			}
			return t * gamma(x);
		} else {
			return Double.POSITIVE_INFINITY;
		}
	}

	public static double lnGamma(double x) {
		if (x <= 0) {
			throw new RuntimeException("para is out of range!");
		}
		double t = ckLanczos[0];
		for (int i = 1; i < ckLanczos.length; i++) {
			t += ckLanczos[i] / (x + i - 1);
		}
		double s = x + gLanczos - 0.5;
		return (x - 0.5) * Math.log(s) - s + Math.log(Math.sqrt(2 * Math.PI) * t);
	}

	public static double lowerRegularizedIncompleteGammaFunction(double x, double a) {
		double tol = 1.0e-15;
		double term = 1 / (a);
		double sum = 0.0;
		for (int i = 1; i <= Math.max(1000, a); ++i) {
			sum += term;
			term *= x / (a + i);
			if (term < tol) {
				break;
			}
		}
		return Math.exp(-x + a * Math.log(x) - XMath.lnGamma(a)) * sum;
	}

	public static double upperRegularizedIncompleteGammaFunction(double x, double a) {
		double minValue = 1.0e-35;
		double tol = 1.0e-15;
		double c = x + 1 - a;
		double f = 1 / c;
		double g = 1.0 / minValue;
		double result = f;
		double tn;
		double mul;
		for (int i = 1; i <= Math.max(1000, a); ++i) {
			tn = i * (a - i);
			c += 2;
			f = tn * f + c;
			if (Math.abs(f) < minValue) {
				f = minValue;
			}
			f = 1 / f;
			g = c + tn / g;
			if (Math.abs(g) < minValue) {
				g = minValue;
			}
			mul = f * g;
			if (Math.abs(f * g - 1) < tol) {
				break;
			}
			result *= mul;
		}
		return Math.exp(-x + a * Math.log(x) - XMath.lnGamma(a)) * result;
	}

	public static double beta(double x, double y) {
		if ((x <= 0) || (y <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		return Math.exp(lnGamma(x) + lnGamma(y) - lnGamma(x + y));
	}

	public static double regularizedIncompleteBetaFunction(double x, double a, double b) {
		if ((a <= 0) || (b <= 0) || (x < 0) || (x > 1)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (x > (double) (a + 1) / (double) (a + b + 2)) {
			return 1 - subRegularizedIncompleteBetaFunction(1 - x, b, a);
		} else {
			return subRegularizedIncompleteBetaFunction(x, a, b);
		}
	}

	private static double subRegularizedIncompleteBetaFunction(double x, double a, double b) {
		double minValue = 1.0e-35;
		double tol = 1e-15;
		double f = 1 + getCoef4RegularizedIncompleteBetaFunction(1, x, a, b);
		if (Math.abs(f) < minValue) {
			f = minValue;
		}
		f = 1 / f;
		double g = 1;
		double r = f * g;
		for (int i = 2; i < Math.max(1000, Math.max(a, b)); ++i) {
			if (Math.abs(f * g - 1) < tol) {
				break;
			}
			f = 1 + f * getCoef4RegularizedIncompleteBetaFunction(i, x, a, b);
			if (Math.abs(f) < minValue) {
				f = minValue;
			}
			f = 1 / f;
			g = 1 + getCoef4RegularizedIncompleteBetaFunction(i, x, a, b) / g;
			if (Math.abs(g) < minValue) {
				g = minValue;
			}
			r *= f * g;
		}
		return Math.exp(lnGamma(a + b) - lnGamma(a) - lnGamma(b) + a * Math.log(x) + b * Math.log(1.0 - x)) * r / a;
	}

	private static double getCoef4RegularizedIncompleteBetaFunction(int m, double x, double a, double b) {
		if (0 == m % 2) {
			int k = m / 2;
			return (k * (b - k) * x) / ((a + m - 1) * (a + m));
		} else {
			int k = m / 2;
			return -((a + k) * (a + b + k) * x) / ((a + m - 1) * (a + m));
		}
	}

	public static double erf(double x) {
		//return Math.signum(x) * CDF.gamma(x * x, 0.5, 1.0);
		GammaDistribution distribution = new GammaDistribution(0.5, 1.0);
		return Math.signum(x) * distribution.cumulativeProbability(x * x);
	}

	public static double erfInverse(double x) {
		GammaDistribution gamma = new GammaDistribution(0.5, 1.0);
		return Math.signum(x) * Math.sqrt(gamma.inverseCumulativeProbability(Math.abs(x)));
	}

	public static double round(double v, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException(
				"The scale must be a positive integer or zero");
		}
		BigDecimal b = new BigDecimal(Double.toString(v));
		BigDecimal one = new BigDecimal("1");
		return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	public int signum(BigInteger x) {
		return x.signum();
	}

}
