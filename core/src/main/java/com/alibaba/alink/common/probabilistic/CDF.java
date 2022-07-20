package com.alibaba.alink.common.probabilistic;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.utils.XMath;
import com.alibaba.alink.operator.common.statistics.DistributionFuncName;

/**
 * 累积分布函数(Cumulative Distribution Function, 缩写为CDF)
 *
 */
public class CDF {

	private DistributionFuncName funcName;
	private double[] params = null;

	public CDF(DistributionFuncName funcName, double[] params) {
		this.funcName = funcName;
		if (null != params) {
			this.params = params.clone();
		}
	}

	/***
	 * 均匀分布的累积分布函数
	 *
	 * @param x             自变量值
	 * @param lowerBound    均匀分布的下界
	 * @param upperBound    均匀分布的上界
	 * @return 累积分布值
	 */
	public static double uniform(double x, double lowerBound, double upperBound) {
		if (lowerBound >= upperBound) {
			throw new AkIllegalOperatorParameterException("Wrong input parameters: the lower bound should be less than the upper bound!");
		}
		if (x < lowerBound) {
			return 0.0;
		} else if (x >= upperBound) {
			return 1.0;
		} else {
			return (x - lowerBound) / (upperBound - lowerBound);
		}
	}

	/***
	 * 指数分布的累积分布函数
	 * 当x>=0时，概率密度函数 f(x) = lambda * exp( -lambda * x) ；否则f(x)=0
	 * 其中 lambda>0
	 *
	 * @param x     自变量值
	 * @paramlamda 指数分布参数
	 * @return 累积分布函数值
	 */
	public static double exponential(double x, double lambda) {
		if ((lambda <= 0) || (x < 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		if (x <= 0) {
			return 0;
		} else {
			return 1 - Math.exp(-lambda * x);
		}
	}

	/***
	 * 标准正态分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @return 累积分布函数值
	 */
	public static double stdNormal(double x) {
		if (Double.NEGATIVE_INFINITY == x) {
			return 0;
		}
		if (Double.POSITIVE_INFINITY == x) {
			return 1;
		}
		if (x == 0) {
			return 0.5;
		}

		return (1.0 + XMath.erf(x / Math.sqrt(2.0))) / 2.0;

	}

	/***
	 * 正态分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param mu        均值
	 * @param sigma2    方差
	 * @return 累积分布函数值
	 */
	public static double normal(double x, double mu, double sigma2) {
		if (sigma2 <= 0) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		double t = (x - mu) / Math.sqrt(sigma2);
		return stdNormal(t);
	}

	/***
	 * Gamma分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param alpha     参数
	 * @param lambda    参数
	 * @return 累积分布函数值
	 */
	public static double gamma(double x, double alpha, double lambda) {
		if (alpha <= 0 || lambda <= 0 || x < 0) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		if (x == Double.POSITIVE_INFINITY) {
			return 1;
		}
		x = x * lambda;
		if (x < 1 + alpha) {
			return XMath.lowerRegularizedIncompleteGammaFunction(x, alpha);
		} else {
			return 1 - XMath.upperRegularizedIncompleteGammaFunction(x, alpha);
		}
	}

	/***
	 * Beta分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param a         参数
	 * @param b         参数
	 * @return 累积分布函数值
	 */
	public static double beta(double x, double a, double b) {
		return XMath.regularizedIncompleteBetaFunction(x, a, b);
	}

	/***
	 * 卡方分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param df        自由度
	 * @return 累积分布函数值
	 */
	public static double chi2(double x, double df) {
		return gamma(x, df / 2.0, 0.5);
	}

	/***
	 * 学生T分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param df        自由度
	 * @return 累积分布函数值
	 */
	public static double studentT(double x, double df) {
		if (df <= 0) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		double t = (x + Math.sqrt(x * x + df)) / (2 * Math.sqrt(x * x + df));
		return beta(t, df / 2, df / 2);
	}

	/***
	 * F分布的累积分布函数
	 *
	 * @param x         自变量值
	 * @param df1       自由度1
	 * @param df2       自由度2
	 * @return 累积分布函数值
	 */
	public static double F(double x, double df1, double df2) {
		if ((df1 <= 0) || (df2 <= 0) || (x < 0)) {
			throw new AkIllegalOperatorParameterException("Input parameter out of range!");
		}
		return beta((df1 * x) / (df1 * x + df2), df1 / 2, df2 / 2);
	}

	public double calculate(double x) {
		switch (funcName) {
			case StdNormal:
				return stdNormal(x);
			case Normal:
				return normal(x, params[0], params[1]);
			case Gamma:
				return gamma(x, params[0], params[1]);
			case Beta:
				return beta(x, params[0], params[1]);
			case Chi2:
				return chi2(x, params[0]);
			case StudentT:
				return studentT(x, params[0]);
			case Uniform:
				return uniform(x, params[0], params[1]);
			case Exponential:
				return exponential(x, params[0]);
			case F:
				return F(x, params[0], params[1]);
			default:
				throw new AkUnsupportedOperationException("Not supported yet!");
		}
	}

}
