package com.alibaba.alink.common.annotation;

import org.junit.Test;

import static com.alibaba.alink.common.annotation.PortDesc.PORT_DESC_CN_BUNDLE;
import static com.alibaba.alink.common.annotation.PortDesc.PORT_DESC_EN_BUNDLE;

public class PortDescTest {
	@Test
	public void testResourceBundle() {
		for (PortDesc value : PortDesc.values()) {
			PORT_DESC_CN_BUNDLE.getString(value.toString());
			PORT_DESC_EN_BUNDLE.getString(value.toString());
		}
	}
}
