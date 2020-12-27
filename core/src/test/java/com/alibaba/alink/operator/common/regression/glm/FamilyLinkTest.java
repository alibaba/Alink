package com.alibaba.alink.operator.common.regression.glm;

import com.alibaba.alink.operator.common.regression.glm.famliy.Binomial;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gamma;
import com.alibaba.alink.operator.common.regression.glm.famliy.Gaussian;
import com.alibaba.alink.operator.common.regression.glm.famliy.Poisson;
import com.alibaba.alink.operator.common.regression.glm.famliy.Tweedie;
import com.alibaba.alink.operator.common.regression.glm.link.CLogLog;
import com.alibaba.alink.operator.common.regression.glm.link.Identity;
import com.alibaba.alink.operator.common.regression.glm.link.Inverse;
import com.alibaba.alink.operator.common.regression.glm.link.Log;
import com.alibaba.alink.operator.common.regression.glm.link.Logit;
import com.alibaba.alink.operator.common.regression.glm.link.Power;
import com.alibaba.alink.operator.common.regression.glm.link.Probit;
import com.alibaba.alink.operator.common.regression.glm.link.Sqrt;
import com.alibaba.alink.params.regression.GlmTrainParams;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class FamilyLinkTest {
	@Test
	public void test() {
		FamilyLink familyLink1 = new FamilyLink(GlmTrainParams.Family.Binomial, 0, null, 0);
		assertEquals(new Logit().name(), familyLink1.getLink().name());
		assertEquals(new Binomial().name(), familyLink1.getFamily().name());
		assertEquals(new Binomial().name(), familyLink1.getFamilyName());
		assertEquals(new Logit().name(), familyLink1.getLinkName());
		assertEquals(-2.197224577336219, familyLink1.predict(0.1));
		assertEquals(0.52497918747894, familyLink1.fitted(0.1));
		assertArrayEquals(new double[] {4.0, 5.0, 42.0, 1.1102230246251563E-16, 1.0},
			familyLink1.calcWeightAndLabel(new double[] {2.0, 6.0}, 3.0, new double[] {4.0, 5.0, 1.0, 1.0, 1.0}),
			10e-8);

		FamilyLink familyLink2 = new FamilyLink(GlmTrainParams.Family.Gamma, 0, null, 0);
		assertEquals(new Inverse().name(), familyLink2.getLink().name());

		FamilyLink familyLink3 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, null, 0);
		assertEquals(new Identity().name(), familyLink3.getLink().name());

		FamilyLink familyLink4 = new FamilyLink(GlmTrainParams.Family.Poisson, 0, null, 0);
		assertEquals(new Log().name(), familyLink4.getLink().name());

		FamilyLink familyLink5 = new FamilyLink(GlmTrainParams.Family.Tweedie, 1, null, 1);
		assertEquals(new Power(1).name(), familyLink5.getLink().name());

		FamilyLink familyLink6 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.CLogLog, 0);
		assertEquals(new CLogLog().name(), familyLink6.getLink().name());

		FamilyLink familyLink7 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Identity, 0);
		assertEquals(new Identity().name(), familyLink7.getLink().name());

		FamilyLink familyLink8 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Inverse, 0);
		assertEquals(new Inverse().name(), familyLink8.getLink().name());

		FamilyLink familyLink9 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Log, 0);
		assertEquals(new Log().name(), familyLink9.getLink().name());

		FamilyLink familyLink10 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Logit, 0);
		assertEquals(new Logit().name(), familyLink10.getLink().name());

		FamilyLink familyLink11 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Power, 2);
		assertEquals(new Power(2).name(), familyLink11.getLink().name());

		FamilyLink familyLink12 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Probit, 0);
		assertEquals(new Probit().name(), familyLink12.getLink().name());

		FamilyLink familyLink13 = new FamilyLink(GlmTrainParams.Family.Gaussian, 0, GlmTrainParams.Link.Sqrt, 0);
		assertEquals(new Sqrt().name(), familyLink13.getLink().name());

		try {
			FamilyLink familyLink14 = new FamilyLink(null, 0, GlmTrainParams.Link.Sqrt, 0);
		} catch (Exception ex) {
			assertEquals("family can not be empty", ex.getMessage());
		}

	}

	@Test
	public void testBinomial() {
		Binomial binomial = new Binomial();
		assertEquals("Binomial", binomial.name());

		double y = 0.1;
		double mu = 0.2;
		double weight = 0.3;
		assertEquals(0.02201400842085035, binomial.deviance(y, mu, weight));
		assertEquals(0.4076923076923077, binomial.initialize(y, weight));
		assertEquals(0.2, binomial.project(mu));
		assertEquals(0.16000000000000003, binomial.variance(mu));

		y = 0;
		assertEquals(0.13388613078852585, binomial.deviance(y, mu, weight));

		mu = 1E-17;
		assertEquals(1.0E-16, binomial.project(mu));

		mu = 1.0;
		assertEquals(0.9999999999999999, binomial.project(mu));

		weight = 1;
		y = -0.5;
		try {
			binomial.initialize(y, weight);
		} catch (Exception ex) {
			assertEquals("mu must be in (0, 1).", ex.getMessage());
		}

		y = 2;
		try {
			binomial.initialize(y, weight);
		} catch (Exception ex) {
			assertEquals("mu must be in (0, 1).", ex.getMessage());
		}
	}

	@Test
	public void testGamma() {
		Gamma gamma = new Gamma();
		assertEquals("Gamma", gamma.name());

		double y = 0.1;
		double mu = 0.2;
		double weight = 0.3;

		assertEquals(0.11588830833596717, gamma.deviance(y, mu, weight));
		assertEquals(0.1, gamma.initialize(y, weight));
		assertEquals(0.2, gamma.project(mu));
		assertEquals(0.04000000000000001, gamma.variance(mu));

		y = 0;
		try {
			gamma.initialize(y, weight);
		} catch (Exception ex) {
			assertEquals("y of Gamma family must be positive.", ex.getMessage());
		}

	}

	@Test
	public void testGaussian() {
		Gaussian gamma = new Gaussian();
		assertEquals("Gaussian", gamma.name());

		double y = 0.1;
		double mu = 0.2;
		double weight = 0.3;

		assertEquals(0.003, gamma.deviance(y, mu, weight));
		assertEquals(0.1, gamma.initialize(y, weight));
		assertEquals(0.2, gamma.project(mu));
		assertEquals(1.0, gamma.variance(mu));

		mu = Double.POSITIVE_INFINITY + 1;
		assertEquals(Double.MAX_VALUE, gamma.project(mu));
		mu = Double.NEGATIVE_INFINITY - 1;
		assertEquals(Double.MIN_VALUE, gamma.project(mu));
	}

	@Test
	public void testPoisson() {
		Poisson poisson = new Poisson();
		assertEquals("Poisson", poisson.name());

		double y = 0.1;
		double mu = 0.2;
		double weight = 0.3;

		assertEquals(0.01841116916640329, poisson.deviance(y, mu, weight));
		assertEquals(0.1, poisson.initialize(y, weight));
		assertEquals(0.2, poisson.project(mu));
		assertEquals(0.2, poisson.variance(mu));

		y = -1;
		try {
			poisson.initialize(y, weight);
		} catch (Exception ex) {
			assertEquals("y must be larger or equal with 0 when Poisson.", ex.getMessage());
		}
	}

	@Test
	public void testTweedie() {
		double variancePower = 0.85;
		Tweedie tweedie = new Tweedie(variancePower);
		assertEquals("Tweedie", tweedie.name());

		double y = 0.1;
		double mu = 0.2;
		double weight = 0.3;

		assertEquals(0.014002785630308991, tweedie.deviance(y, mu, weight));
		assertEquals(0.1, tweedie.initialize(y, weight));
		assertEquals(0.2, tweedie.project(mu));
		assertEquals(0.2546100231092847, tweedie.variance(mu));

		y = 0;
		assertEquals(GlmUtil.DELTA, tweedie.initialize(y, weight));

		mu = 1e-17;
		assertEquals(GlmUtil.EPSILON, tweedie.project(mu));
		mu = Double.POSITIVE_INFINITY;
		assertEquals(Double.MAX_VALUE, tweedie.project(mu));
		mu = Double.NEGATIVE_INFINITY;
		assertEquals(1.0E-16, tweedie.project(mu));

		variancePower = 1;
		Tweedie tweedie1 = new Tweedie(variancePower);
		assertEquals(Double.NaN, tweedie1.deviance(y, mu, weight));

		variancePower = 2;
		Tweedie tweedie2 = new Tweedie(variancePower);
		assertEquals(Double.POSITIVE_INFINITY, tweedie2.deviance(y, mu, weight));
	}

	@Test
	public void testCLogLog() {
		CLogLog cLogLog = new CLogLog();
		assertEquals("CLogLog", cLogLog.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(10.545801756699893, cLogLog.derivative(mu));
		assertEquals(-2.2503673273124454, cLogLog.link(mu));
		assertEquals(0.7051836792708419, cLogLog.unlink(eta));
	}

	@Test
	public void testIdentity() {
		Identity identity = new Identity();
		assertEquals("Identity", identity.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(1.0, identity.derivative(mu));
		assertEquals(0.1, identity.link(mu));
		assertEquals(0.2, identity.unlink(eta));
	}

	@Test
	public void testInverse() {
		Inverse inverse = new Inverse();
		assertEquals("Inverse", inverse.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(-99.99999999999999, inverse.derivative(mu));
		assertEquals(10.0, inverse.link(mu));
		assertEquals(5.0, inverse.unlink(eta));
	}

	@Test
	public void testLog() {
		Log log = new Log();
		assertEquals("Log", log.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(10.0, log.derivative(mu));
		assertEquals(-2.3025850929940455, log.link(mu));
		assertEquals(1.2214027581601699, log.unlink(eta));
	}

	@Test
	public void testLogit() {
		Logit logit = new Logit();
		assertEquals("Logit", logit.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(11.111111111111109, logit.derivative(mu));
		assertEquals(-2.197224577336219, logit.link(mu));
		assertEquals(0.549833997312478, logit.unlink(eta));
	}

	@Test
	public void testPower() {
		double linkPower = 2.0;
		Power power = new Power(linkPower);
		assertEquals("Power", power.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(0.2, power.derivative(mu));
		assertEquals(0.010000000000000002, power.link(mu));
		assertEquals(0.4472135954999579, power.unlink(eta));

		linkPower = 0;
		Power power0 = new Power(linkPower);

		assertEquals(10.0, power0.derivative(mu));
		assertEquals(-2.3025850929940455, power0.link(mu));
		assertEquals(1.2214027581601699, power0.unlink(eta));
	}

	@Test
	public void testProbit() {
		Probit probit = new Probit();
		assertEquals("Probit", probit.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(5.6980598561170055, probit.derivative(mu));
		assertEquals(-1.2815515655446008, probit.link(mu));
		assertEquals(0.579259709439103, probit.unlink(eta));
	}

	@Test
	public void testSqrt() {
		Sqrt sqrt = new Sqrt();
		assertEquals("Sqrt", sqrt.name());

		double mu = 0.1;
		double eta = 0.2;
		assertEquals(1.5811388300841895, sqrt.derivative(mu));
		assertEquals(0.31622776601683794, sqrt.link(mu));
		assertEquals(0.04000000000000001, sqrt.unlink(eta));
	}

}