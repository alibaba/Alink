package com.alibaba.alink.params.validators;

import org.apache.flink.ml.api.misc.param.ParamValidator;

import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;

import java.io.Serializable;

/**
 * Basic definition of Parameter Validator.
 *
 * @param <T> the type of the parameter value
 */
public class Validator<T> implements Serializable, ParamValidator <T> {
	private static final long serialVersionUID = 663170659560352711L;
	protected String paramName;

	@Override
	public boolean validate(T v) {
		return true;
	}

	@Override
	public void validateThrows(T value) {
		StringBuilder sbd = new StringBuilder()
			.append(value);

		if (this.paramName != null && !this.paramName.isEmpty()) {
			sbd.append(" of ")
				.append(this.paramName);
		}

		sbd.append(" is not validate. ")
			.append(toString());

		if (!validate(value)) {
			throw new AkUnclassifiedErrorException(sbd.toString());
		}
	}

	public void setParamName(String paraName) {
		this.paramName = paraName;
	}

}
