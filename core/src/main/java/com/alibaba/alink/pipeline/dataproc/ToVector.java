package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.ToVectorMapper;
import com.alibaba.alink.params.dataproc.ToVectorParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Transforming to vector.
 */
@NameCn("转向量")
public class ToVector extends MapTransformer <ToVector>
	implements ToVectorParams <ToVector> {

	public ToVector() {
		this(new Params());
	}

	public ToVector(Params params) {
		super(ToVectorMapper::new, params);
	}

}
