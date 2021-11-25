package com.alibaba.alink.common.io.plugin;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for {@link DistributeCacheGenerator}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface DistributeCacheGeneratorPolicy {

	/**
	 * The policy of a {@link DistributeCacheGenerator} should be unique from others.
	 *
	 * @return policy.
	 */
	String policy();
}
