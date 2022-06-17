package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceHopWindowStatFeatures;

public class HopWindowCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <HopWindowCategoricalStatFeatures>
	implements InterfaceHopWindowStatFeatures {
	public String[] windowTimes;
	public String[] hopTimes;

	public HopWindowCategoricalStatFeatures() {
		super();
	}

	public HopWindowCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public HopWindowCategoricalStatFeatures setHopTimes(String... hopTimes) {
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
