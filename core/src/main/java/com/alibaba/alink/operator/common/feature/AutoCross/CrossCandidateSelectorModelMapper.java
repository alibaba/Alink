package com.alibaba.alink.operator.common.feature.AutoCross;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.params.feature.featuregenerator.HasAppendOriginalData;

public class CrossCandidateSelectorModelMapper extends AutoCrossModelMapper {

	public CrossCandidateSelectorModelMapper(TableSchema modelSchema,
											 TableSchema dataSchema,
											 Params params) {
		super(modelSchema, dataSchema, params);
		appendOriginalVec = params.get(HasAppendOriginalData.APPEND_ORIGINAL_DATA);
	}
}
