package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTumbleWindowStatFeatures;

public class TumbleWindowCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <TumbleWindowCategoricalStatFeatures>
	implements InterfaceTumbleWindowStatFeatures {
	public String[] windowTimes;

	public TumbleWindowCategoricalStatFeatures() {
		super();
	}

	public TumbleWindowCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}
}
