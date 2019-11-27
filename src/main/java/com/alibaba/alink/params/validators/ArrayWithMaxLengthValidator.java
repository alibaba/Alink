package com.alibaba.alink.params.validators;

import org.apache.flink.ml.api.misc.param.ParamValidator;

public class ArrayWithMaxLengthValidator<T> implements ParamValidator <T[]> {
	final Integer maxLength;

	public ArrayWithMaxLengthValidator(int maxLength) {
		this.maxLength = maxLength;
	}

	@Override
	public boolean validate(T[] v) {
		if (v.length > maxLength) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "lengthOfArray <= " + maxLength;
	}
}
