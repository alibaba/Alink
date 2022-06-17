package com.alibaba.alink.common.fe.def.over;

import com.alibaba.alink.common.fe.def.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.def.InterfaceNStatFeatures;

public class LatestNCategoricalStatFeatures
	extends BaseCategoricalStatFeatures <LatestNCategoricalStatFeatures>
	implements InterfaceNStatFeatures {
	public int[] numbers;

	public LatestNCategoricalStatFeatures() {
		super();
	}

	public LatestNCategoricalStatFeatures setNumbers(int... numbers) {
		this.numbers = numbers;
		return this;
	}

	@Override
	public int[] getNumbers() {
		return numbers;
	}
}
