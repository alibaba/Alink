package com.alibaba.alink.common.io.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation of IO operators.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface IoOpAnnotation {

    /**
     * Datasource name. For example, sqlite, mysql, etc.
     */
    String name();

    /**
     * Does the targeting data source or data sink has a timestamp column by default.
     */
    boolean hasTimestamp() default false;

    /**
     * The {@link IOType} of the annotated operator.
     */
    IOType ioType();
}
