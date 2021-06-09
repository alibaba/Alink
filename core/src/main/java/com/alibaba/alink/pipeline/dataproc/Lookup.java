package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.params.dataproc.LookupParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * Key to Values operation with map model.
 */
public class Lookup extends MapModel <Lookup>
	implements LookupParams<Lookup> {

	public Lookup() {
		this(null);
	}

	public Lookup(Params params) {
		super(LookupModelMapper::new, params);
	}

	@Override
	public Lookup setModelData(BatchOperator <?> modelData) {
		return super.setModelData(modelData);
	}

}