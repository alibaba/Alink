package com.alibaba.alink.params.similarity;

import com.alibaba.alink.params.shared.tree.HasSeed;

/**
 * Params for StringTextApprox.
 */
public interface StringTextApproxParams<T> extends
	HasNumBucket <T>,
	HasNumHashTablesDV10 <T>,
	HasSeed <T> {
}
