package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.InterfaceHopWindowStatFeatures;
import com.alibaba.alink.common.fe.def.BaseNumericStatFeatures;

public class HopWindowNumericStatFeatures extends BaseNumericStatFeatures <HopWindowNumericStatFeatures>
	implements InterfaceHopWindowStatFeatures {
	public String[] windowTimes;
	public String[] hopTimes;

	public HopWindowNumericStatFeatures() {
		super();
	}

	public HopWindowNumericStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public HopWindowNumericStatFeatures setHopTimes(String... hopTimes) {
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
