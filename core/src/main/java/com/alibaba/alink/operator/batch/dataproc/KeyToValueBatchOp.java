package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.KeyToValueModelMapper;
import com.alibaba.alink.params.dataproc.KeyToValueParams;

/**
 * key to value.
 */
public class KeyToValueBatchOp extends ModelMapBatchOp <KeyToValueBatchOp>
	implements KeyToValueParams <KeyToValueBatchOp> {

	private static final long serialVersionUID = 5069468816582686897L;

	public KeyToValueBatchOp() {
		this(null);
	}

	public KeyToValueBatchOp(Params params) {
		super(KeyToValueModelMapper::new, params);
	}
}




