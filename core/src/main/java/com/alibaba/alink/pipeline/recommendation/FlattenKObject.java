package com.alibaba.alink.pipeline.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.FlattenKObjectMapper;
import com.alibaba.alink.params.recommendation.FlattenKObjectParams;
import com.alibaba.alink.pipeline.FlatMapTransformer;

/**
 * Transform json format recommendation to table format.
 */
public class FlattenKObject extends FlatMapTransformer <FlattenKObject>
	implements FlattenKObjectParams <FlattenKObject> {

	private static final long serialVersionUID = 7947293962304088416L;

	public FlattenKObject() {
		this(new Params());
	}

	public FlattenKObject(Params param) {
		super(FlattenKObjectMapper::new, param);
	}

}
