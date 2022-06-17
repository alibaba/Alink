package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSessionWindowStatFeatures;

public class SessionWindowNumericStatFeatures extends BaseNumericStatFeatures <SessionWindowNumericStatFeatures>
	implements InterfaceSessionWindowStatFeatures {
	public String[] sessionGapTimes;

	public SessionWindowNumericStatFeatures() {
		super();
	}

	public SessionWindowNumericStatFeatures setSessionGapTimes(String... sessionGapTimes) {
		this.sessionGapTimes = sessionGapTimes;
		return this;
	}

	@Override
	public String[] getSessionGapTimes() {
		return sessionGapTimes;
	}
}
