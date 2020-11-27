package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.dataproc.KeyToValueModelMapper;
import com.alibaba.alink.params.dataproc.KeyToValueParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Key to Value operation with map model.
 */
public class KeyToValue extends MapModel <KeyToValue>
	implements KeyToValueParams <KeyToValue> {

	private static final long serialVersionUID = 729925124242823642L;

	public KeyToValue() {
		this(null);
	}

	public KeyToValue(Params params) {
		super(KeyToValueModelMapper::new, params);
	}

}