package com.alibaba.alink.common.fe.define.over;

import com.alibaba.alink.common.fe.define.BaseCategoricalStatFeatures;
import com.alibaba.alink.common.fe.define.InterfaceNStatFeatures;

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
