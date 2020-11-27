package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.utils.Functional;
import com.alibaba.alink.operator.common.similarity.dataConverter.NearestNeighborDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.StringModelDataConverter;
import com.alibaba.alink.operator.common.similarity.dataConverter.VectorModelDataConverter;
import com.alibaba.alink.params.similarity.StringTextApproxNearestNeighborTrainParams;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;

public enum TrainType {
	VECTOR(params -> new VectorModelDataConverter()),

	APPROX_VECTOR(params -> params.get(VectorApproxNearestNeighborTrainParams.SOLVER).getDataConverter()),

	STRING(params -> new StringModelDataConverter()),

	APPROX_STRING(params -> params.get(StringTextApproxNearestNeighborTrainParams.METRIC).getDataConverter()),

	TEXT(params -> new StringModelDataConverter()),

	APPROX_TEXT(params -> params.get(StringTextApproxNearestNeighborTrainParams.METRIC).getDataConverter());

	public Functional.SerializableFunction <Params, NearestNeighborDataConverter> dataConverter;

	TrainType(Functional.SerializableFunction <Params, NearestNeighborDataConverter> dataConverter) {
		this.dataConverter = dataConverter;
	}

}
