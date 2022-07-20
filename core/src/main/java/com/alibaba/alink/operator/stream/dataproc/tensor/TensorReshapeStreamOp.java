package com.alibaba.alink.operator.stream.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.dataproc.tensor.TensorReshapeMapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.dataproc.tensor.TensorReshapeParams;

@NameCn("张量重组")
public final class TensorReshapeStreamOp extends MapStreamOp <TensorReshapeStreamOp>
	implements TensorReshapeParams <TensorReshapeStreamOp> {

	private static final long serialVersionUID = 3670185631952980397L;

	public TensorReshapeStreamOp() {
		this(null);
	}

	public TensorReshapeStreamOp(Params param) {
		super(TensorReshapeMapper::new, param);

	}
}
