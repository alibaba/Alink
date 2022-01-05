package com.alibaba.alink.common.probabilistic;

import com.alibaba.alink.common.utils.XMath;
import com.alibaba.alink.operator.common.statistics.DistributionFuncName;

/**
 * 累积分布函数的反函数(Inverse cumulative Distribution Function, 缩写为IDF)
 *
 */
public class IDF {

	private DistributionFuncName funcName;
	private double[] params = null;

	public IDF(DistributionFuncName funcName, double[] params) {
		this.funcName = funcName;
		if (null != params) {
			this.params = params.clone();
		}
	}

	/***
	 * 标准正态分布的反函数
	 *
	 * @param p 自变量值
	 * @return 反函数的值
	 */
	public static double stdNormal(double p) {
		return Math.sqrt(2.0) * XMath.erfInverse(2 * p - 1);
	}

	/***
	 * 正态分布的反函数
	 *
	 * @param p         自变量值
	 * @param mu        均值
	 * @param sigma2    方差
	 * @return 反函数的值
	 */
	public static double normal(double p, double mu, double sigma2) {
		if ((p < 0) || (p > 1) || (sigma2 < 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		return stdNormal(p) * Math.sqrt(sigma2) + mu;
	}

	/***
	 * Gamma分布的反函数
	 *
	 * @param p         自变量值
	 * @param alpha     参数
	 * @param lambda    参数
	 * @return 反函数的值
	 */
	public static double gamma(double p, double alpha, double lambda) {
		if ((alpha <= 0) || (lambda <= 0) || (p > 1) && (p < 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (1.0 == p) {
			return Double.POSITIVE_INFINITY;
		}
		if (0.0 == p) {
			return 0;
		}

		double x = 1 + alpha;

		double h, t;
		for (int i = 0; i < 1000; i++) {
			t = CDF.gamma(x, alpha, 1) - p;
			if (Math.abs(t) < 1e-15) {
				break;
			}
			h = t / PDF.gamma(x, alpha, 1);
			x = Math.min(x * 5, Math.max(x / 5, x - h));
			if (Math.abs(h) < 1e-15 * x) {
				break;
			}
		}
		return x / lambda;
	}

	/***
	 * Beta分布的反函数
	 *
	 * @param p         自变量值
	 * @param a         参数
	 * @param b         参数
	 * @return 反函数的值
	 */
	public static double beta(double p, double a, double b) {
		if ((p < 0) || (p > 1) || (a <= 0) || (b <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}

		if ((0.0 == p) || (1.0 == p)) {
			return p;
		}

		double x = (a + 1) / (a + b + 2);

		double h, t;
		for (int i = 0; i < 1000; i++) {
			t = CDF.beta(x, a, b) - p;
			if (Math.abs(t) < 1e-15) {
				break;
			}
			h = t / PDF.beta(x, a, b);
			x = Math.max(x / 5, Math.min(1 - (1 - x) / 5, x - h));
			if (Math.abs(h) < 1e-15 * x) {
				break;
			}
		}
		return x;
	}

	/***
	 * 卡方分布的反函数
	 *
	 * @param p         自变量值
	 * @param df        自由度
	 * @return 反函数的值
	 */
	public static double chi2(double p, double df) {
		return (gamma(p, df / 2.0, 0.5));
	}

	/***
	 * 学生T分布的反函数
	 *
	 * @param p         自变量值
	 * @param df        自由度
	 * @return 反函数的值
	 */
	public static double studentT(double p, double df) {
		if ((p <= 0) || (p >= 1) || (df <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (df == 1) {
			return (Math.tan(Math.PI * (p - 0.5)));
		}
		double q = p - 0.5;
		double b = beta(2 * Math.abs(q), 0.5, df / 2);
		double z = 1 - b;
		return Math.signum(q) * Math.sqrt(df * b / z);
	}

	/***
	 * F分布的反函数
	 *
	 * @param p         自变量值
	 * @param df1       自由度1
	 * @param df2       自由度2
	 * @return 反函数的值
	 */
	public static double F(double p, double df1, double df2) {
		if ((p < 0) || (p >= 1) || (df1 <= 0) || (df2 <= 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		double x;
		if (p > 0) {
			double z = beta(p, df1 / 2, df2 / 2);
			x = df2 * z / (df1 * (1 - z));
		} else {
			x = 0;
		}
		return x;
	}

	/***
	 * 均匀分布
	 *
	 * @param p             自变量值
	 * @param lowerBound    均匀分布的下界
	 * @param upperBound    均匀分布的上界
	 * @return 反函数的值
	 */
	public static double uniform(double p, double lowerBound, double upperBound) {
		if (lowerBound >= upperBound) {
			throw new RuntimeException("Wrong input parameters: the lower bound should be less than the upper bound!");
		}
		if ((p < 0) || (p > 1)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		return Math.min(p * (upperBound - lowerBound) + lowerBound, upperBound);
	}

	/***
	 * 指数分布
	 * 当x>=0时，概率密度函数 f(x) = lambda * exp( -lambda * x) ；否则f(x)=0
	 * 其中 lambda>0
	 *
	 * @param p     自变量值
	 * @param lamda 指数分布参数
	 * @return 反函数的值
	 */
	public static double exponential(double p, double lambda) {
		if ((lambda <= 0) || (p < 0)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if ((p < 0) || (p > 1)) {
			throw new RuntimeException("Input parameter out of range!");
		}
		if (0 == p) {
			return 0;
		} else if (1.0 == p) {
			return Double.POSITIVE_INFINITY;
		} else {
			return -Math.log(1.0 - p) / lambda;
		}
	}

	public double calculate(double p) {
		switch (funcName) {
			case StdNormal:
				return stdNormal(p);
			case Normal:
				return normal(p, params[0], params[1]);
			case Gamma:
				return gamma(p, params[0], params[1]);
			case Beta:
				return beta(p, params[0], params[1]);
			case Chi2:
				return chi2(p, params[0]);
			case StudentT:
				return studentT(p, params[0]);
			case Uniform:
				return uniform(p, params[0], params[1]);
			case Exponential:
				return exponential(p, params[0]);
			case F:
				return F(p, params[0], params[1]);
			default:
				throw new RuntimeException("Not supported yet!");
		}
	}
}
