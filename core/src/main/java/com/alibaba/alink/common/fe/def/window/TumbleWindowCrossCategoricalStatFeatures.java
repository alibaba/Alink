package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceTumbleWindowStatFeatures;

public class TumbleWindowCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <TumbleWindowCrossCategoricalStatFeatures>
	implements InterfaceTumbleWindowStatFeatures {
	public String[] windowTimes;

	public TumbleWindowCrossCategoricalStatFeatures() {
		super();
	}

	public TumbleWindowCrossCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}
}
