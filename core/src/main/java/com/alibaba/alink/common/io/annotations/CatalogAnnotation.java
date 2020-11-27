package com.alibaba.alink.common.io.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation of DB.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface CatalogAnnotation {

	/**
	 * Datasource name. For example, sqlite, mysql, etc.
	 *
	 * @return
	 */
	String name();

	/**
	 * Does the database has a timestamp column by default.
	 *
	 * @return
	 */
	boolean hasTimestamp() default false;
}
