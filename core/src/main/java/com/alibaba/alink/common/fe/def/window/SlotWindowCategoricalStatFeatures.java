package com.alibaba.alink.common.fe.def.window;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceSlotWindowStatFeatures;

public class SlotWindowCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <SlotWindowCategoricalStatFeatures>
	implements InterfaceSlotWindowStatFeatures {
	public String[] windowTimes;
	public String[] stepTimes;

	public SlotWindowCategoricalStatFeatures() {
		super();
	}

	public SlotWindowCategoricalStatFeatures setWindowTimes(String... windowTimes) {
		this.windowTimes = windowTimes;
		return this;
	}

	public SlotWindowCategoricalStatFeatures setStepTimes(String... stepTimes) {
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
