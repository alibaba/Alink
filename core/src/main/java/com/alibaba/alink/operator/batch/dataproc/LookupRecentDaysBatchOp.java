package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.SelectedColsWithSecondInputSpec;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.dataproc.LookupRecentDaysModelMapper;
import com.alibaba.alink.params.dataproc.LookupRecentDaysParams;

/**
 * key to values.
 */
@SelectedColsWithSecondInputSpec
@NameCn("表查找")
@NameEn("Lookup Recent Days Table")
public class LookupRecentDaysBatchOp extends ModelMapBatchOp <LookupRecentDaysBatchOp>
	implements LookupRecentDaysParams <LookupRecentDaysBatchOp> {

	public LookupRecentDaysBatchOp() {
		this(null);
	}

	public LookupRecentDaysBatchOp(Params params) {
		super(LookupRecentDaysModelMapper::new, params);
	}

}




