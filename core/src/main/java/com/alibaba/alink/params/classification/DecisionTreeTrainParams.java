package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.shared.tree.HasIndividualTreeType;

public interface DecisionTreeTrainParams<T> extends
	IndividualTreeParams <T>,
	HasIndividualTreeType <T> {
}
