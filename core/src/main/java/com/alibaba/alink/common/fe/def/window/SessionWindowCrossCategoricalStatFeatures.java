package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSessionWindowStatFeatures;

public class SessionWindowCrossCategoricalStatFeatures
	extends BaseCrossCategoricalStatFeatures <SessionWindowCrossCategoricalStatFeatures>
	implements InterfaceSessionWindowStatFeatures {
	public String[] sessionGapTimes;

	public SessionWindowCrossCategoricalStatFeatures() {
		super();
	}

	public SessionWindowCrossCategoricalStatFeatures setSessionGapTimes(String... sessionGapTimes) {
		this.sessionGapTimes = sessionGapTimes;
		return this;
	}

	@Override
	public String[] getSessionGapTimes() {
		return sessionGapTimes;
	}
}
