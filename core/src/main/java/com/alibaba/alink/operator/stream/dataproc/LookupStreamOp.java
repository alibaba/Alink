package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.dataproc.LookupParams;

/**
 * stream op for lookup.
 */
@ParamSelectColumnSpec(name="mapKeyCols",portIndices = 0)
@ParamSelectColumnSpec(name="mapValueCols",portIndices = 0)
@SelectedColsWithSecondInputSpec
@NameCn("表查找")
@NameEn("Lookup Operation")
public class LookupStreamOp extends ModelMapStreamOp <LookupStreamOp>
	implements LookupParams<LookupStreamOp> {

	public LookupStreamOp() {
		super(LookupModelMapper::new, new Params());
	}

	public LookupStreamOp(Params params) {
		super(LookupModelMapper::new, params);
	}

	public LookupStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LookupStreamOp(BatchOperator model, Params params) {
		super(model, LookupModelMapper::new, params);
	}

}
