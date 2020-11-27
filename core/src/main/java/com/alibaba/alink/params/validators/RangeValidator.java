package com.alibaba.alink.params.validators;

public class RangeValidator<T extends Comparable <T>> extends Validator <T> {
	private static final long serialVersionUID = -383763066683649679L;
	T minVal;
	T maxVal;
	boolean leftInclusive = true;
	boolean rightInclusive = true;
	boolean isNullValid = false;

	public RangeValidator(T minVal, T maxVal) {
		this.minVal = minVal;
		this.maxVal = maxVal;
	}

	public RangeValidator <T> setLeftInclusive(boolean tag) {
		this.leftInclusive = tag;
		return this;
	}

	public RangeValidator <T> setRightInclusive(boolean tag) {
		this.rightInclusive = tag;
		return this;
	}

	public RangeValidator <T> setNullValid(boolean nullValid) {
		isNullValid = nullValid;
		return this;
	}

	@Override
	public boolean validate(T v) {
		if (v == null) {
			return isNullValid;
		}
		if (leftInclusive) {
			if (!(minVal.compareTo(v) <= 0)) {
				return false;
			}
		} else {
			if (!(minVal.compareTo(v) < 0)) {
				return false;
			}
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
		sb.append("value in ");
		sb.append(leftInclusive ? "[" : "(");
		sb.append(minVal);
		sb.append(", ");
		sb.append(maxVal);
		sb.append(rightInclusive ? "]" : ")");
		return sb.toString();
	}
}
