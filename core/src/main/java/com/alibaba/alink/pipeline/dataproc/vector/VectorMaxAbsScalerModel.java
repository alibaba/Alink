package com.alibaba.alink.pipeline.dataproc.vector;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.vector.VectorMaxAbsScalerModelMapper;
import com.alibaba.alink.params.dataproc.vector.VectorMaxAbsScalerPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Vector MaxAbs pipeline model.
 */
@NameCn("向量绝对值最大化模型")
public class VectorMaxAbsScalerModel extends MapModel <VectorMaxAbsScalerModel>
	implements VectorMaxAbsScalerPredictParams <VectorMaxAbsScalerModel> {

	private static final long serialVersionUID = -7377073086403532200L;

	public VectorMaxAbsScalerModel(Params params) {
		super(VectorMaxAbsScalerModelMapper::new, params);
	}
}
