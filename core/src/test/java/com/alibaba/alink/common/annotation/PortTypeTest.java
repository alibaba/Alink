package com.alibaba.alink.common.annotation;

import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import static com.alibaba.alink.common.annotation.PortType.PORT_TYPE_CN_BUNDLE;
import static com.alibaba.alink.common.annotation.PortType.PORT_TYPE_EN_BUNDLE;

public class PortTypeTest extends AlinkTestBase {
	@Test
	public void testResourceBundle() {
		for (PortType value : PortType.values()) {
			PORT_TYPE_CN_BUNDLE.getString(value.toString());
			PORT_TYPE_EN_BUNDLE.getString(value.toString());
		}
	}
}
