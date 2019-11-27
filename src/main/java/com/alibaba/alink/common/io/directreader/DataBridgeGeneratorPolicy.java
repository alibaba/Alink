package com.alibaba.alink.common.io.directreader;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for {@link DataBridgeGenerator}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface DataBridgeGeneratorPolicy {

	/**
	 * The policy of a {@link DataBridgeGenerator} should be unique from others.
	 * @return policy.
	 */
	String policy();
}
