package com.alibaba.alink.common.probabilistic;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.utils.XMath;

/**
 * 概率密度函数(Probability Density Function, 缩写为PDF)
 *
 */
public class PDF {

	public static double poisson(int n, double lambda) {
		if ((lambda <= 0) || (n < 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
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

	/***
	 * 均匀分布的概率密度函数
	 *
	 * @param x             自变量值
	 * @param lowerBound    均匀分布的下界
	 * @param upperBound    均匀分布的上界
	 * @return 概率密度值
	 */
	public static double uniform(double x, double lowerBound, double upperBound) {
		if (lowerBound >= upperBound) {
			throw new AkIllegalOperatorParameterException("Wrong input parameters: the lower bound should be less than the upper bound!");
		}
		if (x < lowerBound || x > upperBound) {
			return 0.0;
		} else {
			return 1 / (upperBound - lowerBound);
		}
	}

	/***
	 * 指数分布的概率密度函数
	 * 当x>=0时，概率密度函数 f(x) = lambda * exp( -lambda * x) ；否则f(x)=0
	 * 其中 lambda>0
	 *
	 * @param x     自变量值
	 * @param lamda 指数分布参数
	 * @return 概率密度值
	 */
	public static double exponential(double x, double lambda) {
		if (lambda <= 0) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		if (x < 0) {
			return 0;
		} else {
			return lambda * Math.exp(-lambda * x);
		}
	}

	/***
	 * 标准正态分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @return 概率密度值
	 */
	public static double stdNormal(double x) {
		return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
	}

	/***
	 * 正态分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param mu        均值
	 * @param sigma2    方差
	 * @return 概率密度值
	 * @throws Exception
	 */
	public static double normal(double x, double mu, double sigma2) throws Exception {
		if (sigma2 <= 0) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		double sigma = Math.sqrt(sigma2);
		double t = (x - mu) / sigma;
		return stdNormal(t) / sigma;
	}

	/***
	 * 卡方分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param df        自由度
	 * @return 概率密度值
	 */
	public static double chi2(double x, double df) {
		if (x <= 0) {
			return 0.0;
		} else {
			double n2 = (double) df / 2;
			return Math.exp(-x / 2 + (n2 - 1) * Math.log(x) - n2 * Math.log(2.0) - XMath.lnGamma(n2));
		}
	}

	/***
	 * 学生T分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param df        自由度
	 * @return 概率密度值
	 */
	public static double studentT(double x, double df) {
		return Math.exp(-(df + 1.0) * Math.log(1 + x * x / df) / 2.0 + XMath.lnGamma((df + 1) / 2.0) - XMath
			.lnGamma(df / 2.0) - Math.log(df) / 2.0 - Math.log(Math.PI) / 2.0);
	}

	/***
	 * F分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param df1       自由度1
	 * @param df2       自由度2
	 * @return 概率密度值
	 */
	public static double F(double x, double df1, double df2) {
		if (x <= 0) {
			return 0.0;
		} else {
			return Math.exp(df1 * (Math.log(df1) - Math.log(df2)) / 2.0 + (df1 / 2.0 - 1) * Math.log(x)
				- (df1 + df2) * Math.log(1.0 + x * df1 / df2) / 2.0 - XMath.lnGamma(df2 / 2.0) - XMath
				.lnGamma(df1 / 2.0) + XMath.lnGamma((df1 + df2) / 2.0));
		}
	}

	/***
	 * Gamma分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param alpha     参数
	 * @param lambda    参数
	 * @return 概率密度值
	 */
	public static double gamma(double x, double alpha, double lambda) {
		if ((alpha <= 0) || (lambda <= 0) || (x <= 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		return Math.exp(alpha * Math.log(lambda) + (alpha - 1) * Math.log(x) - lambda * x - XMath.lnGamma(alpha));
	}

	/***
	 * Beta分布的概率密度函数
	 *
	 * @param x         自变量值
	 * @param a         参数
	 * @param b         参数
	 * @return 概率密度值
	 */
	public static double beta(double x, double a, double b) {
		if ((x < 0) || (x > 1) || (a <= 0) || (b <= 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		return Math.exp(
			XMath.lnGamma(a + b) - XMath.lnGamma(a) - XMath.lnGamma(b) + (a - 1) * Math.log(x) + (b - 1) * Math
				.log(1 - x));
	}

	public static double fisherZ(double x, double p, double q) {
		if ((p <= 0) || (q <= 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		if (x <= 0) {
			return 0;
		} else {
			double r = (p - 1) * Math.log(x) - (p + q) * Math.log(1 + x);
			return Math.exp(r - XMath.lnGamma(p) - XMath.lnGamma(q) + XMath.lnGamma(p + q));
		}
	}
}
