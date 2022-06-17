package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTumbleWindowStatFeatures;

public class TumbleWindowNumericStatFeatures extends BaseNumericStatFeatures <TumbleWindowNumericStatFeatures>
	implements InterfaceTumbleWindowStatFeatures {
	public String[] windowTimes;

	public TumbleWindowNumericStatFeatures() {
		super();
	}

	public TumbleWindowNumericStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}
}
