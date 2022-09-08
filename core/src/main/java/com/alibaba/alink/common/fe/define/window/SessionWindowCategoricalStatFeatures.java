package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceSessionWindowStatFeatures;

public class SessionWindowCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <SessionWindowCategoricalStatFeatures>
	implements InterfaceSessionWindowStatFeatures {
	public String[] sessionGapTimes;

	public SessionWindowCategoricalStatFeatures() {
		super();
	}

	public SessionWindowCategoricalStatFeatures setSessionGapTimes(String... sessionGapTimes) {
		this.sessionGapTimes = sessionGapTimes;
		return this;
	}

	@Override
	public String[] getSessionGapTimes() {
		return sessionGapTimes;
	}
}
