package com.alibaba.alink.operator.common.tree;

import org.junit.Assert;
import org.junit.Test;

public class CriteriaTest {
	private final static Criteria.Gini GINI
		= new Criteria.Gini(10.0, 10, new double[] {2.0, 8.0});
	private final static Criteria.Gini LEFT_GINI
		= new Criteria.Gini(2.0, 2, new double[] {2.0, 0.0});
	private final static Criteria.Gini RIGHT_GINI
		= new Criteria.Gini(8.0, 8, new double[] {0.0, 8.0});

	private final static Criteria.InfoGain INFO_GAIN
		= new Criteria.InfoGain(10.0, 10, new double[] {2.0, 8.0});
	private final static Criteria.InfoGain LEFT_INFO_GAIN
		= new Criteria.InfoGain(2.0, 2, new double[] {2.0, 0.0});
	private final static Criteria.InfoGain RIGHT_INFO_GAIN
		= new Criteria.InfoGain(8.0, 8, new double[] {0.0, 8.0});

	private final static Criteria.InfoGainRatio INFO_GAIN_RATIO
		= new Criteria.InfoGainRatio(10.0, 10, new double[] {2.0, 8.0});
	private final static Criteria.InfoGainRatio LEFT_INFO_GAIN_RATIO
		= new Criteria.InfoGainRatio(2.0, 2, new double[] {2.0, 0.0});
	private final static Criteria.InfoGainRatio RIGHT_INFO_GAIN_RATIO
		= new Criteria.InfoGainRatio(8.0, 8, new double[] {0.0, 8.0});

	private final static Criteria.MSE MSE
		= new Criteria.MSE(10.0, 10, 10.0, 10.0);
	private final static Criteria.MSE LEFT_MSE
		= new Criteria.MSE(2.0, 2, 2.0, 2.0);
	private final static Criteria.MSE RIGHT_MSE
		= new Criteria.MSE(8.0, 8, 8.0, 8.0);

	private final static double EPS = 1e-6;

	@Test
	public void toLabelCounter() {
		Assert.assertEquals(GINI.toLabelCounter().getNumInst(), GINI.getNumInstances());
		Assert.assertEquals(GINI.toLabelCounter().getWeightSum(), GINI.getWeightSum(), EPS);
		Assert.assertArrayEquals(GINI.toLabelCounter().getDistributions(), new double[] {2.0, 8.0}, EPS);

		Assert.assertEquals(INFO_GAIN.toLabelCounter().getNumInst(), INFO_GAIN.getNumInstances());
		Assert.assertEquals(INFO_GAIN.toLabelCounter().getWeightSum(), INFO_GAIN.getWeightSum(), EPS);
		Assert.assertArrayEquals(INFO_GAIN.toLabelCounter().getDistributions(), new double[] {2.0, 8.0}, EPS);

		Assert.assertEquals(INFO_GAIN_RATIO.toLabelCounter().getNumInst(), INFO_GAIN_RATIO.getNumInstances());
		Assert.assertEquals(INFO_GAIN_RATIO.toLabelCounter().getWeightSum(), INFO_GAIN_RATIO.getWeightSum(), EPS);
		Assert.assertArrayEquals(INFO_GAIN_RATIO.toLabelCounter().getDistributions(), new double[] {2.0, 8.0}, EPS);

		Assert.assertEquals(MSE.toLabelCounter().getNumInst(), MSE.getNumInstances());
		Assert.assertEquals(MSE.toLabelCounter().getWeightSum(), MSE.getWeightSum(), EPS);
		Assert.assertArrayEquals(MSE.toLabelCounter().getDistributions(), new double[] {10.0, 10.0}, EPS);
	}

	@Test
	public void gain() {
		Assert.assertEquals(
			1.0 - (0.2 * 0.2) - (0.8 * 0.8),
			GINI.gain(LEFT_GINI, RIGHT_GINI),
			EPS
		);

		Assert.assertEquals(
			-0.2 * Math.log(0.2) / Math.log(2)
				- 0.8 * Math.log(0.8) / Math.log(2)
				+ 0.2 * 1.0 * Math.log(1.0) / Math.log(2)
				+ 0.8 * 1.0 * Math.log(1.0) / Math.log(2),
			INFO_GAIN.gain(LEFT_INFO_GAIN, RIGHT_INFO_GAIN),
			EPS
		);

		Assert.assertEquals(
			(
				(-0.2 * Math.log(0.2) / Math.log(2)
					- 0.8 * Math.log(0.8) / Math.log(2)
					+ 0.2 * 1.0 * Math.log(1.0) / Math.log(2)
					+ 0.8 * 1.0 * Math.log(1.0) / Math.log(2))
					/ (-0.2 * Math.log(0.2) / Math.log(2)
					- 0.8 * Math.log(0.8) / Math.log(2))
			),
			INFO_GAIN_RATIO.gain(LEFT_INFO_GAIN_RATIO, RIGHT_INFO_GAIN_RATIO),
			EPS
		);

		Assert.assertEquals(
			(10.0 / 10.0 - 10.0 / 10.0 * 10.0 / 10.0)
				- 0.2 * (2.0 / 2.0 - 2.0 / 2.0 * 2.0 / 2.0)
				- 0.8 * (8.0 / 8.0 - 8.0 / 8.0 * 8.0 / 8.0),
			MSE.gain(LEFT_MSE, RIGHT_MSE),
			EPS
		);
	}

	@Test
	public void subtract() {
		Criteria.Gini subGini = (Criteria.Gini) GINI.clone().subtract(LEFT_GINI.clone());

		Assert.assertEquals(RIGHT_GINI.getNumInstances(), subGini.getNumInstances());
		Assert.assertEquals(RIGHT_GINI.getWeightSum(), subGini.getWeightSum(), EPS);
		Assert.assertArrayEquals(RIGHT_GINI.distributions, subGini.distributions, EPS);

		Criteria.InfoGain subInfoGain = (Criteria.InfoGain) INFO_GAIN.clone().subtract(LEFT_INFO_GAIN.clone());

		Assert.assertEquals(RIGHT_INFO_GAIN.getNumInstances(), subInfoGain.getNumInstances());
		Assert.assertEquals(RIGHT_INFO_GAIN.getWeightSum(), subInfoGain.getWeightSum(), EPS);
		Assert.assertArrayEquals(RIGHT_INFO_GAIN.distributions, subInfoGain.distributions, EPS);

		Criteria.InfoGainRatio subInfoGainRatio
			= (Criteria.InfoGainRatio) INFO_GAIN_RATIO.clone().subtract(LEFT_INFO_GAIN_RATIO.clone());

		Assert.assertEquals(RIGHT_INFO_GAIN_RATIO.getNumInstances(), subInfoGainRatio.getNumInstances());
		Assert.assertEquals(RIGHT_INFO_GAIN_RATIO.getWeightSum(), subInfoGainRatio.getWeightSum(), EPS);
		Assert.assertArrayEquals(RIGHT_INFO_GAIN_RATIO.distributions, subInfoGainRatio.distributions, EPS);

		Criteria.MSE subMSE
			= (Criteria.MSE) MSE.clone().subtract(LEFT_MSE.clone());

		Assert.assertEquals(RIGHT_MSE.getNumInstances(), subMSE.getNumInstances());
		Assert.assertEquals(RIGHT_MSE.getWeightSum(), subMSE.getWeightSum(), EPS);
		Assert.assertEquals(RIGHT_MSE.getSum(), subMSE.getSum(), EPS);
		Assert.assertEquals(RIGHT_MSE.getSquareSum(), subMSE.getSquareSum(), EPS);
	}
}