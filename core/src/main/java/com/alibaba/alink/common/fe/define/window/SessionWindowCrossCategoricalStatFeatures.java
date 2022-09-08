package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseCrossCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceSessionWindowStatFeatures;

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
