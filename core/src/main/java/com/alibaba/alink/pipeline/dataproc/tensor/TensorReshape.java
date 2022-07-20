package com.alibaba.alink.pipeline.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.tensor.TensorReshapeMapper;
import com.alibaba.alink.params.dataproc.tensor.TensorReshapeParams;
import com.alibaba.alink.pipeline.MapTransformer;

@NameCn("张量重组")
public class TensorReshape extends MapTransformer <TensorReshape>
	implements TensorReshapeParams <TensorReshape> {

	public TensorReshape() {
		this(null);
	}

	public TensorReshape(Params params) {
		super(TensorReshapeMapper::new, params);
	}
}
