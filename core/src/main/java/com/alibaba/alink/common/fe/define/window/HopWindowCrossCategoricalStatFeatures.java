package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceHopWindowStatFeatures;

public class HopWindowCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <HopWindowCrossCategoricalStatFeatures>
	implements InterfaceHopWindowStatFeatures {
	public String[] windowTimes;
	public String[] hopTimes;

	public HopWindowCrossCategoricalStatFeatures() {
	}

	public HopWindowCrossCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public HopWindowCrossCategoricalStatFeatures setHopTimes(String... hopTimes) {
		this.hopTimes = hopTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}

	@Override
	public String[] getHopTimes() {
		return hopTimes;
	}
}
