package com.alibaba.alink.params.validators;

public class ArrayWithMaxLengthValidator<T> extends Validator <T[]> {
	private static final long serialVersionUID = -1883428012126683112L;
	final Integer maxLength;
	boolean isNullValid = false;

	public ArrayWithMaxLengthValidator(int maxLength) {
		this.maxLength = maxLength;
	}

	public ArrayWithMaxLengthValidator <T> setNullValid(boolean nullValid) {
		isNullValid = nullValid;
		return this;
	}

	@Override
	public boolean validate(T[] v) {
		if (v == null) {
			return isNullValid;
		}
		return v.length <= maxLength;
	}

	@Override
	public String toString() {
		return "lengthOfArray <= " + maxLength;
	}

}
