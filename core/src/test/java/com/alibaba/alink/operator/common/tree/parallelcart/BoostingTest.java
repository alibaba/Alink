package com.alibaba.alink.operator.common.tree.parallelcart;

import com.alibaba.alink.operator.common.tree.parallelcart.booster.GradientBaseBooster;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.LogLoss;
import org.junit.Assert;
import org.junit.Test;

public class BoostingTest {
	@Test
	public void testGradient() {
		GradientBaseBooster booster
			= new GradientBaseBooster(
			new LogLoss(2.0, 1.0),
			new double[] {1.0, 1.0, 1.0},
			new Slice(0, 3)
		);

		booster.boosting(null, new double[] {1, 1, 0}, new double[] {0.5, 0.5, 0.1});

		Assert.assertArrayEquals(new double[] {0.3775406687981454, 0.3775406687981454, -0.52497918747894},
			booster.getGradients(), 1e-6);
		Assert.assertArrayEquals(new double[] {0.1425369565965509, 0.1425369565965509, 0.27560314728604807},
			booster.getGradientsSqr(), 1e-6);
		Assert.assertNull(booster.getHessions());
		Assert.assertArrayEquals(new double[] {1.0, 1.0, 1.0}, booster.getWeights(), 1e-6);
	}
}