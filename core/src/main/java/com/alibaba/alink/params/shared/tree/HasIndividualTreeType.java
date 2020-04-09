package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasIndividualTreeType<T> extends WithParams<T> {
	ParamInfo<TreeType> TREE_TYPE = ParamInfoFactory
		.createParamInfo("treeType", TreeType.class)
		.setDescription("The criteria of the tree. " +
			"There are three options: gini, infoGain, infoGainRatio"
		)
		.setHasDefaultValue(TreeType.GINI)
		.build();

	default TreeType getTreeType() {
		return get(TREE_TYPE);
	}

	default T setTreeType(TreeType value) {
		return set(TREE_TYPE, value);
	}

	default T setTreeType(String value) {
		return set(TREE_TYPE, ParamUtil.searchEnum(TREE_TYPE, value));
	}

	/**
	 * Indicate that the criteria using in the decision tree.
	 */
	enum TreeType {
		/**
		 * Gini index. ref: cart.
		 */
		GINI,

		/**
		 * Info gain. ref: id3
		 */
		INFOGAIN,

		/**
		 * Info gain ratio. ref: c4.5
		 */
		INFOGAINRATIO
	}
}