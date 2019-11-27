package com.alibaba.alink.params.validators;

import org.apache.flink.ml.api.misc.param.ParamValidator;

import java.io.Serializable;

/**
 * Basic definition of Parameter Validator.
 *
 * @param <T> the type of the parameter value
 */
public class Validator<T> implements Serializable, ParamValidator <T> {
	static <U extends Comparable <U>> RangeValidator <U> inRange(U minValue, U maxValue) {
		return new RangeValidator <>(minValue, maxValue);
	}

	@Override
	public boolean validate(T v) {
		return true;
	}
}
