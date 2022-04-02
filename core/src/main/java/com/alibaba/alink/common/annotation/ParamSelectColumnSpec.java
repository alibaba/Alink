package com.alibaba.alink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Repeatable(ParamSelectColumnSpecs.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface ParamSelectColumnSpec {
	String name();

	int[] portIndices() default {};

	boolean defaultSelectAll() default false;

	//String[] allowedTypeStrs() default {};

	TypeCollections[] allowedTypeCollections() default {};
}
