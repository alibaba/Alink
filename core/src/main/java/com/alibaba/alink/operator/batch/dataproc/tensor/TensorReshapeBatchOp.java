package com.alibaba.alink.operator.batch.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.MapBatchOp;
import com.alibaba.alink.operator.common.dataproc.tensor.TensorReshapeMapper;
import com.alibaba.alink.params.dataproc.tensor.TensorReshapeParams;

@NameCn("张量重组")
public final class TensorReshapeBatchOp extends MapBatchOp <TensorReshapeBatchOp>
	implements TensorReshapeParams <TensorReshapeBatchOp> {

	private static final long serialVersionUID = 6671189574524574429L;

	public TensorReshapeBatchOp() {
		this(null);
	}

	public TensorReshapeBatchOp(Params params) {
		super(TensorReshapeMapper::new, params);
	}

}
