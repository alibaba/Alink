package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.pipeline.PipelineStageBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ParamGrid.
 */
public class ParamGrid implements Serializable {

	private List<Tuple3<PipelineStageBase, ParamInfo, Object[]>> items = new ArrayList<>();

	public <V> ParamGrid addGrid(PipelineStageBase stage, ParamInfo<V> info, V[] params) {
		Preconditions.checkNotNull(params, "Parameter should not be null.");
		Preconditions.checkArgument(params.length > 0, "The length of parameter should not be empty.");

		this.items.add(Tuple3.of(stage, info, params));

		return this;
	}

	public <V> ParamGrid addGrid(PipelineStageBase stage, String info, V[] params) throws Exception {
		Preconditions.checkNotNull(params, "Parameter should not be null.");
		Preconditions.checkArgument(params.length > 0, "The length of parameter should not be empty.");

		this.items.add(Tuple3.of(stage, ParamInfoFactory.createParamInfo(info, params[0].getClass()).build(), params));

		return this;
	}

	public List<Tuple3<PipelineStageBase, ParamInfo, Object[]>> getItems() {
		return items;
	}

}
