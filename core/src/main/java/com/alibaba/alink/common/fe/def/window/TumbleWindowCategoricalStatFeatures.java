package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTumbleWindowStatFeatures;

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
