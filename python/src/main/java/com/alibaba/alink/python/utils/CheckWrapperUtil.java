package com.alibaba.alink.python.utils;

import org.apache.flink.ml.api.misc.param.WithParams;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Check a Java class and its corresponding Python class are consistent.
 */
@SuppressWarnings("unused")
public class CheckWrapperUtil {

	private static final Set <String> OBJECT_METHOD_NAME_SET = new HashSet <>(Arrays.asList(
		"getClass", "wait", "notifyAll", "notify", "hashCode", "equals", "toString"
	));

	private static final Set <String> WITH_PARAMS_METHOD_NAME_SET = new HashSet <>(Arrays.asList(
		"set", "get", "getParams"
	));

	/**
	 * Get public methods names in a class, excluding some inherited from parent classes.
	 *
	 * @param jClassName class name
	 * @return method name list
	 * @throws ClassNotFoundException
	 */
	public static List <String> getJMethodNames(String jClassName) throws ClassNotFoundException {
		Class <?> cls = Class.forName(jClassName);
		return Arrays.stream(cls.getMethods())
			.filter(d -> Modifier.isPublic(d.getModifiers()))
			.map(Method::getName)
			.filter(d -> !OBJECT_METHOD_NAME_SET.contains(d))
			.filter(d -> !(WithParams.class.isAssignableFrom(cls) && WITH_PARAMS_METHOD_NAME_SET.contains(d)))
			.collect(Collectors.toList());
	}
}
