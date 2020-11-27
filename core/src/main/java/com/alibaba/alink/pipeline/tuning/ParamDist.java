package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.pipeline.PipelineStageBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ParamDist implements Serializable {

	private static final long serialVersionUID = 2754119603352684042L;
	private List <Tuple3 <PipelineStageBase, ParamInfo, ValueDist>> items = new ArrayList <>();

	public ParamDist addDist(PipelineStageBase stage, ParamInfo info, ValueDist dist) throws Exception {
		this.items.add(Tuple3.of(stage, info, dist));

		return this;
	}

	public List <Tuple3 <PipelineStageBase, ParamInfo, ValueDist>> getItems() {
		return items;
	}

}
