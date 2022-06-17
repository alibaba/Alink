package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.LookupHBaseMapper;
import com.alibaba.alink.params.dataproc.LookupHBaseParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Lookup operation from HBase.
 */
@NameCn("查找HBase数据")
public class LookupHBase extends MapTransformer <LookupHBase>
	implements LookupHBaseParams <LookupHBase> {

	public LookupHBase() {
		this(null);
	}

	public LookupHBase(Params params) {
		super(LookupHBaseMapper::new, params);
	}

}
