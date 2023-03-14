package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.common.statistics.DistributionFuncName;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

class ValueDistFunc extends ValueDist <Double> {

	private static final long serialVersionUID = -3106023232687585706L;
	private AbstractRealDistribution dist;
	private double mean, sd;

	public ValueDistFunc(DistributionFuncName funcName, double[] params) {
		switch (funcName) {
			case StdNormal:
				dist = new NormalDistribution(0, 1);
				mean = 0;
				sd = 1;
				break;
			case Normal:
				dist = new NormalDistribution(params[0], params[1]);
				mean = params[0];
				sd = params[1];
				break;
			case Gamma:
				dist = new GammaDistribution(params[0], params[1]);
				break;
			case Beta:
				dist = new BetaDistribution(params[0], params[1]);
				break;
			case Chi2:
				dist = new ChiSquaredDistribution(params[0]);
				break;
			case StudentT:
				dist = new TDistribution(params[0]);
				break;
			case Uniform:
				dist = new UniformRealDistribution(params[0], params[1]);
				mean = (params[0] + params[1]) / 2;
				sd = params[1] - params[0];
				break;
			case Exponential:
				dist = new ExponentialDistribution(params[0]);
				break;
			case F:
				dist = new FDistribution(params[0], params[1]);
				break;
			default:
				throw new RuntimeException("Not supported yet!");
		}
	}

	@Override
	public Double get(double p) {
		return this.dist.inverseCumulativeProbability(p);
	}

	public double getMean() {return mean;}

	public double getSd() {return sd;}

}
