package com.alibaba.alink.common.fe.define.window;

import com.alibaba.alink.common.fe.define.BaseNumericStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceSlotWindowStatFeatures;

public class SlotWindowNumericStatFeatures extends BaseNumericStatFeatures <SlotWindowNumericStatFeatures>
	implements InterfaceSlotWindowStatFeatures {
	public String[] windowTimes;
	public String[] stepTimes;

	public SlotWindowNumericStatFeatures() {
		super();
	}

	public SlotWindowNumericStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public SlotWindowNumericStatFeatures setStepTimes(String... stepTimes) {
		this.stepTimes = stepTimes;
		return this;
	}

	@Override
	public String[] getWindowTimes() {
		return windowTimes;
	}

	@Override
	public String[] getStepTimes() {
		return stepTimes;
	}
}
