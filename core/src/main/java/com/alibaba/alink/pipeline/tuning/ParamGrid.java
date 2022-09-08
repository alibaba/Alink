package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.pipeline.PipelineStageBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ParamGrid.
 */
public class ParamGrid implements Serializable {

	private static final long serialVersionUID = 5681488864448356257L;
	private List <Tuple3 <PipelineStageBase, ParamInfo, Object[]>> items = new ArrayList <>();

	public <V> ParamGrid addGrid(PipelineStageBase stage, ParamInfo <V> info, V[] params) {
		AkPreconditions.checkNotNull(params, "Parameter should not be null.");
		AkPreconditions.checkArgument(params.length > 0, "The length of parameter should not be empty.");

		this.items.add(Tuple3.of(stage, info, params));

		return this;
	}

	@Deprecated
	public <V> ParamGrid addGrid(PipelineStageBase stage, String info, V[] params) throws Exception {
		AkPreconditions.checkNotNull(params, "Parameter should not be null.");
		AkPreconditions.checkArgument(params.length > 0, "The length of parameter should not be empty.");

		this.items.add(Tuple3.of(stage, ParamInfoFactory.createParamInfo(info, params[0].getClass()).build(), params));

		return this;
	}

	public List <Tuple3 <PipelineStageBase, ParamInfo, Object[]>> getItems() {
		return items;
	}

}
