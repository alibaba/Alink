package com.alibaba.alink.params.finance;

public interface RegressionSelectorParams<T> extends
	StepwiseSelectorParams <T>,
	HasRegSelectorMethod <T>,
	HasSelectorOptimMethod <T> {

}
