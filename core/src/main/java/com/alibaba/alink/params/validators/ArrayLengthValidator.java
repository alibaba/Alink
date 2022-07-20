package com.alibaba.alink.params.validators;

public class ArrayLengthValidator<T> extends Validator <T[]> {
	private static final long serialVersionUID = -1883428012126683112L;
	final Integer length;
	boolean isNullValid = false;

	public ArrayLengthValidator(int length) {
		this.length = length;
	}

	public ArrayLengthValidator <T> setNullValid(boolean nullValid) {
		isNullValid = nullValid;
		return this;
	}

	@Override
	public boolean validate(T[] v) {
		if (v == null) {
			return isNullValid;
		}
		return v.length == length;
	}

	@Override
	public String toString() {
		return "lengthOfArray = " + length;
	}

}
