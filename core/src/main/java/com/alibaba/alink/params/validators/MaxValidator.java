package com.alibaba.alink.params.validators;

public class MaxValidator<T extends Comparable <T>> extends Validator <T> {
	private static final long serialVersionUID = 3145207209654795987L;
	T maxVal;
	boolean rightInclusive = true;
	boolean isNullValid = false;

	public MaxValidator(T maxVal) {
		this.maxVal = maxVal;
	}

	public MaxValidator <T> setRightInclusive(boolean rightInclusive) {
		this.rightInclusive = rightInclusive;
		return this;
	}

	public MaxValidator <T> setNullValid(boolean nullValid) {
		isNullValid = nullValid;
		return this;
	}

	@Override
	public boolean validate(T v) {
		if (v == null) {
			return isNullValid;
		}
		if (rightInclusive) {
			return maxVal.compareTo(v) >= 0;
		} else {
			return maxVal.compareTo(v) > 0;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		//sb.append("value in (-inf, ");
		//sb.append(maxVal);
		//sb.append(rightInclusive ? "]" : ")");
		sb.append("x").append(rightInclusive ? " <= " : " < ").append(maxVal);
		return sb.toString();
	}
}
