package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * Trait for ChiSqSelector.
 */
public interface ChiSqSelectorParams<T> extends
	BasedChisqSelectorParams<T>,
	HasSelectedCols <T> {
}
