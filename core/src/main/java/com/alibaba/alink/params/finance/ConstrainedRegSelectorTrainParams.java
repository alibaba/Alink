package com.alibaba.alink.params.finance;

public interface ConstrainedRegSelectorTrainParams<T>
	extends StepwiseSelectorParams <T>,
	HasContrainedOptimMethod <T>,
	HasRegSelectorMethod <T> {
}