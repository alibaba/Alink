package com.alibaba.alink.common.io.directreader;

import org.junit.Assert;
import org.junit.Test;

public class DummyDataBridgeGeneratorTest {

	@Test
	public void generate() {
		Assert.assertTrue(new DummyDataBridgeGenerator().generate(null, null).read(value -> false).isEmpty());
	}
}