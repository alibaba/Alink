package com.alibaba.alink.operator.common.slidingwindow.windowtree;

import java.io.Serializable;

/**
 * @param <T> Inheritance class type
 * @author forzzig
 * Change default clone() method to "public", add deepClone() method(could be empty if not used).
 */
public interface DeepCloneable<T extends DeepCloneable <T>> extends Cloneable, Serializable {
	long serialVersionUID = -1079699151581522539L;

	T deepClone();

	T clone();
}
