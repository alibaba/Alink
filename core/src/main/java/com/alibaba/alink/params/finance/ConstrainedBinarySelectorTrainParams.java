package com.alibaba.alink.params.finance;

public interface ConstrainedBinarySelectorTrainParams<T> extends
	StepwiseSelectorParams <T>,
	HasContrainedOptimMethod <T>,
	HasBinarySelectorMethod <T> {
}
