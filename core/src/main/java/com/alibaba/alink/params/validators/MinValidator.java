package com.alibaba.alink.params.validators;

public class MinValidator<T extends Comparable <T>> extends Validator <T> {
	private static final long serialVersionUID = 414829150291651630L;
	T minVal;
	boolean leftInclusive = true;
	boolean isNullValid = false;

	public MinValidator(T minVal) {
		this.minVal = minVal;
	}

	public MinValidator <T> setLeftInclusive(boolean leftInclusive) {
		this.leftInclusive = leftInclusive;
		return this;
	}

	public MinValidator <T> setNullValid(boolean nullValid) {
		isNullValid = nullValid;
		return this;
	}

	@Override
	public boolean validate(T v) {
		if (v == null) {
			return isNullValid;
		}
		if (leftInclusive) {
			return minVal.compareTo(v) <= 0;
		} else {
			return minVal.compareTo(v) < 0;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		//sb.append("value in \"");
		//sb.append(leftInclusive ? "[" : "(");
		//sb.append(minVal);
		//sb.append(", +inf)\"");
		sb.append("x").append(leftInclusive ? " >= " : " > ").append(minVal);
		return sb.toString();
	}
}
