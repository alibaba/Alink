package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.LookupModelMapper;
import com.alibaba.alink.params.dataproc.LookupParams;

/**
 * key to values.
 */
@SelectedColsWithSecondInputSpec
@NameCn("表查找")
@NameEn("Lookup Table")
public class LookupBatchOp extends ModelMapBatchOp <LookupBatchOp>
	implements LookupParams<LookupBatchOp> {

	public LookupBatchOp() {
		this(null);
	}

	public LookupBatchOp(Params params) {
		super(LookupModelMapper::new, params);
	}
}




