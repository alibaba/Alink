package com.alibaba.alink.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for MLEnvironmentFactory.
 */
public class MLEnvironmentFactoryTest {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidMLEnvId() {
		MLEnvironmentFactory.get(-1L);
	}

	@Test
	public void testGetDefault() {
		MLEnvironment mlEnvironment = MLEnvironmentFactory
			.get(MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID);
		MLEnvironment mlEnvironmentDefault = MLEnvironmentFactory.getDefault();

		Assert.assertSame(mlEnvironment, mlEnvironmentDefault);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSetDefault() {
		MLEnvironment mlEnvironment = MLEnvironmentFactory.getDefault();
		MLEnvironmentFactory.setDefault(mlEnvironment);
	}

	@Test
	public void getNewMLEnvironmentId() {
		Long mlEnvironmentId = MLEnvironmentFactory.getNewMLEnvironmentId();
		Assert.assertNotNull(MLEnvironmentFactory.get(mlEnvironmentId));
	}

	@Test
	public void registerMLEnvironment() {
		MLEnvironment mlEnvironment = new MLEnvironment();
		Long mlEnvironmentId = MLEnvironmentFactory.registerMLEnvironment(mlEnvironment);
		Assert.assertSame(MLEnvironmentFactory.get(mlEnvironmentId), mlEnvironment);
	}

	@Test(expected = IllegalArgumentException.class)
	public void remove() {
		MLEnvironment mlEnvironment = new MLEnvironment();
		Long mlEnvironmentId = MLEnvironmentFactory.registerMLEnvironment(mlEnvironment);
		MLEnvironmentFactory.remove(mlEnvironmentId);
		MLEnvironmentFactory.get(mlEnvironmentId);
	}
}