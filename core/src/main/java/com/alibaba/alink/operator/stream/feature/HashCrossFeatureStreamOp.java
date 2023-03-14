package com.alibaba.alink.operator.stream.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.operator.common.feature.HashCrossFeatureMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.feature.HashCrossFeatureParams;

@ParamSelectColumnSpec(name = "selectedCols")
@NameCn("Hash Cross特征")
@NameEn("Hash cross feature generator")
public class HashCrossFeatureStreamOp extends MapStreamOp <HashCrossFeatureStreamOp>
	implements HashCrossFeatureParams <HashCrossFeatureStreamOp> {

	public HashCrossFeatureStreamOp() {
		this(new Params());
	}

	public HashCrossFeatureStreamOp(Params params) {
		super(HashCrossFeatureMapper::new, params);
	}
}
