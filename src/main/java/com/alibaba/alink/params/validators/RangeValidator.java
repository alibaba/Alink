package com.alibaba.alink.params.validators;

import org.apache.flink.ml.api.misc.param.ParamValidator;

public class RangeValidator<T extends Comparable <T>> implements ParamValidator <T> {
	T minVal;
	T maxVal;
	boolean leftInclusive = true;
	boolean rightInclusive = true;

	public RangeValidator(T minVal, T maxVal) {
		this.minVal = minVal;
		this.maxVal = maxVal;
	}

	public RangeValidator <T> withLeftInclusive(boolean tag) {
		this.leftInclusive = tag;
		return this;
	}

	public RangeValidator <T> withRightInclusive(boolean tag) {
		this.rightInclusive = tag;
		return this;
	}

	public void setLeftInclusive(boolean leftInclusive) {
		this.leftInclusive = leftInclusive;
	}

	public void setRightInclusive(boolean rightInclusive) {
		this.rightInclusive = rightInclusive;
	}

	@Override
	public boolean validate(T v) {
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
			if (!(maxVal.compareTo(v) >= 0)) {
				return false;
			}
		} else {
			if (!(maxVal.compareTo(v) > 0)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("in ");
		sb.append(leftInclusive ? "[" : "(");
		sb.append(minVal);
		sb.append(", ");
		sb.append(rightInclusive ? "]" : ")");
		return sb.toString();
	}
}
