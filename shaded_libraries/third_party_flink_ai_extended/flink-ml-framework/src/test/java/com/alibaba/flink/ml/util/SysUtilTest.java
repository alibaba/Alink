package com.alibaba.flink.ml.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SysUtilTest {

	@Test
	public void getProjectVersion() {
		Assert.assertEquals("0.3.0-SNAPSHOT", SysUtil.getProjectVersion());
	}
}