package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTumbleWindowStatFeatures;

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
