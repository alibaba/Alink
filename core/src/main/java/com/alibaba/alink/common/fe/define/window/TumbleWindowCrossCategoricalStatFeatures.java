package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceTumbleWindowStatFeatures;

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
