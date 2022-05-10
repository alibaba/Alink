package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.operator.common.dataproc.LookupRecentDaysModelMapper;
import com.alibaba.alink.params.dataproc.LookupParams;
import com.alibaba.alink.params.dataproc.LookupRecentDaysParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Key to Values operation with map model.
 */
@NameCn("表查找")
public class LookupRecentDays extends MapModel <LookupRecentDays>
	implements LookupRecentDaysParams <LookupRecentDays> {

	public LookupRecentDays() {
		this(null);
	}

	public LookupRecentDays(Params params) {
		super(LookupRecentDaysModelMapper::new, params);
	}

}
