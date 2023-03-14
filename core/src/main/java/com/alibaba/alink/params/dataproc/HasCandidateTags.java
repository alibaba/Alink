package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasCandidateTags<T> extends WithParams <T> {

	@NameCn("候选特征标签")
	@DescCn("筛选包含指定标签的特征")
	ParamInfo <String[]> CANDIDATE_TAGS = ParamInfoFactory
		.createParamInfo("candidateTags", String[].class)
		.setDescription("retain features if contains tag in candidateTags")
		.setAlias(new String[] {"candidateTags"})
		.setHasDefaultValue(null)
		.build();

	default String[] getCandidateTags() {
		return get(CANDIDATE_TAGS);
	}

	default T setCandidateTags(String... colNames) {
		return set(CANDIDATE_TAGS, colNames);
	}
}

