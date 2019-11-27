package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.tree.HasTreeType;

public interface DecisionTreeTrainParams<T> extends
	IndividualTreeParams<T>,
	HasTreeType<T> {
}
