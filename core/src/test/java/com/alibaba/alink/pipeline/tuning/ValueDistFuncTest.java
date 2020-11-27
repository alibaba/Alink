package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.common.statistics.DistributionFuncName;
import org.junit.Assert;
import org.junit.Test;

public class ValueDistFuncTest {

	@Test
	public void testStdNormal() {
		DistributionFuncName dist = DistributionFuncName.StdNormal;
		double[] params = new double[] {0, 1};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(-1.281552, func.get(0.1), 10e-5);
	}

	@Test
	public void testNormal() {
		DistributionFuncName dist = DistributionFuncName.Normal;
		double[] params = new double[] {-1, 2};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(-3.563103, func.get(0.1), 10e-5);
	}

	@Test
	public void testStudentT() {
		DistributionFuncName dist = DistributionFuncName.StudentT;
		double[] params = new double[] {10};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(-1.372184, func.get(0.1), 10e-5);
	}

	@Test
	public void testBeta() {
		DistributionFuncName dist = DistributionFuncName.Beta;
		double[] params = new double[] {0.5, 0.5};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(0.6545085, func.get(0.6), 10e-5);
	}

	@Test
	public void testGamma() {
		DistributionFuncName dist = DistributionFuncName.Gamma;
		double[] params = new double[] {0.5, 0.5};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(0.1770816, func.get(0.6), 10e-5);
	}

	@Test
	public void testChi2() {
		DistributionFuncName dist = DistributionFuncName.Chi2;
		double[] params = new double[] {10};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(10.47324, func.get(0.6), 10e-5);
	}

	@Test
	public void testUniform() {
		DistributionFuncName dist = DistributionFuncName.Uniform;
		double[] params = new double[] {0, 1};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(0.5, func.get(0.5), 10e-5);
	}

	@Test
	public void testExponential() {
		DistributionFuncName dist = DistributionFuncName.Exponential;
		double[] params = new double[] {0.1};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(0.01053605, func.get(0.1), 10e-5);
	}

	@Test
	public void testF() {
		DistributionFuncName dist = DistributionFuncName.F;
		double[] params = new double[] {10, 2};

		ValueDistFunc func = new ValueDistFunc(dist, params);

		Assert.assertEquals(0.3419428, func.get(0.1), 10e-5);
	}

}