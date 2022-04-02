package com.alibaba.alink.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describe specification of all input ports. Must be declared in pair with {@link InputPorts}.
 * <p>
 * This annotation supports inheritance from the parent class. But if redeclared, redeclare {@link InputPorts} at the
 * same time.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface OutputPorts {
	PortSpec[] values() default {};
}
