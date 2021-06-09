package com.alibaba.alink.common.io.directreader;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class DummyDataBridgeGeneratorTest extends AlinkTestBase {

	@Test
	public void generate() {
		Assert.assertTrue(new DummyDataBridgeGenerator().generate(null, null).read(value -> false).isEmpty());
	}
}