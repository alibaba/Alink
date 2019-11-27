package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Trait for VectorChiSqSelector.
 */
public interface VectorChiSqSelectorParams<T> extends
    BasedChisqSelectorParams<T>,
	HasSelectedCol <T> {
}
