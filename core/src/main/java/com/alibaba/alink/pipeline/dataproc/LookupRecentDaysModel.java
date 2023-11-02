package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.LookupRecentDaysModelMapper;
import com.alibaba.alink.params.dataproc.LookupRecentDaysParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Key to Values operation with map model.
 */
@NameCn("查找近日特征")
public class LookupRecentDaysModel extends MapModel <LookupRecentDaysModel>
	implements LookupRecentDaysParams <LookupRecentDaysModel> {

	public LookupRecentDaysModel() {
		this(null);
	}

	public LookupRecentDaysModel(Params params) {
		super(LookupRecentDaysModelMapper::new, params);
	}

}
