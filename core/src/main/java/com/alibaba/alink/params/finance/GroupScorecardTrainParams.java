package com.alibaba.alink.params.finance;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasGroupCols;
import com.alibaba.alink.params.shared.tree.HasMaxLeaves;
import com.alibaba.alink.params.shared.tree.HasMinSamplesPerLeafDefaultAs500;

public interface GroupScorecardTrainParams<T>
	extends ScorecardTrainParams <T>,
	HasGroupCols <T>,
	HasMaxLeaves <T>,
	HasMinSamplesPerLeafDefaultAs500 <T> {

	@NameCn("叶节点的最小正负样本个数")
	@DescCn("叶节点的最小正负样本个数")
	ParamInfo <Integer> MIN_OUTCOME_SAMPLES_PER_LEAF = ParamInfoFactory
		.createParamInfo("minOutcomeSamplesPerLeaf", Integer.class)
		.setDescription("Minimal number of positive and negative sample in one leaf.")
		.setHasDefaultValue(500)
		.setAlias(new String[] {"minLeafSample"})
		.build();

	default Integer getMinOutcomeSamplesPerLeaf() {
		return get(MIN_SAMPLES_PER_LEAF);
	}

	default T setMinOutcomeSamplesPerLeaf(Integer value) {
		return set(MIN_SAMPLES_PER_LEAF, value);
	}
	@NameCn("树节点分裂标准")
	@DescCn("树节点分裂标准")
	ParamInfo <TreeMeasure> TREE_MEASURE_PARAM_INFO = ParamInfoFactory
		.createParamInfo("treeSplitMeasure", TreeMeasure.class)
		.setDescription("measure for tree split: AUC or KS")
		.setHasDefaultValue(TreeMeasure.AUC)
		.build();

	default TreeMeasure getTreeSplitMeasure() {
		return get(TREE_MEASURE_PARAM_INFO);
	}

	default T setTreeSplitMeasure(TreeMeasure value) {
		return set(TREE_MEASURE_PARAM_INFO, value);
	}

	default T setTreeSplitMeasure(String value) {
		return set(TREE_MEASURE_PARAM_INFO, ParamUtil.searchEnum(TREE_MEASURE_PARAM_INFO, value));
	}

	enum TreeMeasure {
		AUC,
		KS
	}



}
