package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.distance.CategoricalDistance;
import com.alibaba.alink.operator.common.distance.FastCategoricalDistance;

import java.io.Serializable;

/**
 * Interface for different calc method.
 */
public abstract class Similarity<T> implements Serializable, FastCategoricalDistance <T> {
	private static final long serialVersionUID = -4251110127880274044L;
	protected CategoricalDistance distance;

	public CategoricalDistance getDistance() {
		return distance;
	}

	public <M> double calc(M left, M right) {
		if (left instanceof String && right instanceof String) {
			return calc((String) left, (String) right);
		} else if (left instanceof String[] && right instanceof String[]) {
			return calc((String[]) left, (String[]) right);
		} else {
			throw new IllegalArgumentException(
				"only support calculation between string and string or string array and string array!");
		}
	}

	public abstract double calc(String left, String right);

	public abstract double calc(String[] left, String[] right);
}
